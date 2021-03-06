% Copyright 2017-2018 Gregor Uhlenheuer
%
% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
% You may obtain a copy of the License at
%
%     http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.

-module(statser_listener_udp).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

%% API
-export([start_link/1,
         listen/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {config, counters, timers, gauges, sets, flush_timer, prune_timer, filters}).

-define(DELIMITER_PIPE, <<"|">>).
-define(DELIMITER_COLON, <<":">>).
-define(BASE_TIMER_CORRECTION, 50).
-define(UDP_RECEIVE_BUFFER, 524288).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(#udp_config{port=Port, interval=Interval} = Config) ->
    lager:info("starting UDP listener at port ~w with flush interval ~w ms", [Port, Interval]),

    Filters = statser_config:get_metric_filters(),
    gen_server:cast(self(), accept),

    {ok, #state{config=Config,
                counters=maps:new(),
                timers=maps:new(),
                gauges=maps:new(),
                sets=maps:new(),
                filters=Filters}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(accept, #state{config=Config} = State) ->
    Self = self(),
    Port = Config#udp_config.port,
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}, {recbuf, ?UDP_RECEIVE_BUFFER}]),
    lager:info("UDP listener opened port at ~w [~p]", [Port, Socket]),

    % TODO: handle exit/failure
    spawn_link(?MODULE, listen, [Self, Socket]),

    State0 = schedule_flush(State),
    {noreply, schedule_prune(State0)};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({msg, Binary}, State) ->
    Parts = binary:split(Binary, <<"\n">>, [global, trim_all]),
    NewState = lists:foldl(fun handle/2, State, Parts),

    {noreply, NewState};

handle_info(prune, #state{config=Config} = State) ->
    lager:debug("udp: start pruning entries"),

    PruneAfterSecs = Config#udp_config.prune_after div ?MILLIS_PER_SEC,
    PruneBefore = statser_util:seconds() - PruneAfterSecs,

    Counters = maps:filter(fun(_, {TS, _}) -> TS > PruneBefore end, State#state.counters),
    Timers = maps:filter(fun(_, {TS, _, _}) -> TS > PruneBefore end, State#state.timers),
    Gauges = maps:filter(fun(_, {TS, _}) -> TS > PruneBefore end, State#state.gauges),
    Sets = maps:filter(fun(_, {TS, _}) -> TS > PruneBefore end, State#state.sets),

    lager:debug("udp: finished pruning entries"),

    State0 = State#state{counters=Counters,
                         timers=Timers,
                         gauges=Gauges,
                         sets=Sets},

    {noreply, schedule_prune(State0)};

handle_info(flush, #state{config=Config} = State) ->
    Start = erlang:monotonic_time(millisecond),
    Now = statser_util:seconds(),
    PerSecond = Config#udp_config.interval / ?MILLIS_PER_SEC,

    % flush counters
    Counters = maps:map(fun(K, {TS, V}) ->
                                publish(<<"counters.", K/binary, ".count">>, V, Now),
                                publish(<<"counters.", K/binary, ".rate">>, V / PerSecond, Now),
                                {TS, 0}
                        end, State#state.counters),

    % flush timers
    Timers = maps:map(fun(K, {TS, Vs, Cnt}) ->
                              {Min, Max, Mean, Median, StdDev, Sum, SquaredSum} = calculate_timer(Vs),

                              publish(<<"timers.", K/binary, ".min">>, Min, Now),
                              publish(<<"timers.", K/binary, ".max">>, Max, Now),
                              publish(<<"timers.", K/binary, ".mean">>, Mean, Now),
                              publish(<<"timers.", K/binary, ".median">>, Median, Now),
                              publish(<<"timers.", K/binary, ".stddev">>, StdDev, Now),
                              publish(<<"timers.", K/binary, ".sum">>, Sum, Now),
                              publish(<<"timers.", K/binary, ".sum_squares">>, SquaredSum, Now),
                              publish(<<"timers.", K/binary, ".count">>, Cnt, Now),

                              {TS, [], 0}
                      end, State#state.timers),

    % flush gauges
    Gauges = maps:map(fun(K, {TS, V}) ->
                                publish(<<"gauges.", K/binary>>, V, Now),
                                {TS, 0}
                        end, State#state.gauges),

    % flush sets
    Sets = maps:map(fun(K, {TS, V}) ->
                            publish(<<"sets.", K/binary>>, sets:size(sets:from_list(V)), Now),
                            {TS, []}
                    end, State#state.sets),

    Duration = erlang:monotonic_time(millisecond) - Start,
    statser_instrumentation:record(<<"udp.processing-time">>, Duration),

    State0 = State#state{counters=Counters,
                         timers=Timers,
                         gauges=Gauges,
                         sets=Sets},

    {noreply, schedule_flush(State0)};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{flush_timer=FTimer, prune_timer=PTimer}) ->
    lager:info("terminating UDP listener service at ~w", [self()]),
    erlang:cancel_timer(FTimer, [{async, true}]),
    erlang:cancel_timer(PTimer, [{async, true}]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

listen(Parent, Socket) ->
    {ok, {_Addr, _Port, Packet}} = gen_udp:recv(Socket, 0),
    Parent ! {msg, Packet},
    listen(Parent, Socket).


publish(Key, Value, TS) ->
    % TODO: configurable global prefix
    GlobalPrefix = <<"stats.">>,
    Metric = <<GlobalPrefix/binary, Key/binary>>,
    statser_router:line(Metric, Value, TS).


calculate_timer([]) ->
    {0, 0, 0, 0, 0, 0, 0};
calculate_timer(Values) ->
    Sorted = lists:sort(Values),
    Min = hd(Sorted),
    {Sum, SquaredSum, Len} = walk_values(Sorted),
    Mean = Sum / Len,
    {StdDev, Max} = stddev_max(Mean, Sorted),
    Median = statser_calc:median(Sorted),
    {Min, Max, Mean, Median, StdDev, Sum, SquaredSum}.


walk_values([V | Vs]) ->
    walk_values(V, V * V, 1, Vs).

walk_values(A, B, Len, []) ->
    {A, B, Len};
walk_values(A, B, Len, [X | Xs]) ->
    walk_values(A + X, B + X * X, Len+1, Xs).


stddev_max(Mean, Values) ->
    stddev_max(Mean, 0, Values).

stddev_max(Mean, StdDev, [V]) ->
    {StdDev + std_err(Mean, V), V};
stddev_max(Mean, StdDev, [V | Vs]) ->
    stddev_max(Mean, StdDev + std_err(Mean, V), Vs).


std_err(Mean, Value) ->
    Err0 = Value - Mean,
    Err0 * Err0.


handle(Packet, State) ->
    lager:debug("handle UDP packet: ~p", [Packet]),

    case handle_packet(Packet, State) of
        {ok, Updated} ->
            statser_instrumentation:increment(<<"udp.handled-values">>),
            Updated;
        error ->
            statser_instrumentation:increment(<<"udp.invalid-metrics">>),
            State
    end.


handle_packet(Packet, State) ->
    case binary:split(Packet, ?DELIMITER_COLON, [global, trim_all]) of
        [Metric | Rest] ->
            case statser_config:metric_passes_filters(Metric, State#state.filters) of
                true ->
                    handle_metrics(Metric, Rest, State);
                false ->
                    statser_instrumentation:increment(<<"metrics-blacklisted">>),
                    {ok, State}
            end;
        _ ->
            % TODO: check for valid metrics name
            handle_metric(State, Packet)
    end.


handle_metrics(Metric, Rest, State) ->
    lists:foldl(fun (_Bit, error) -> error;
                    (Bit, {ok, S0}) ->
                        Split = binary:split(Bit, ?DELIMITER_PIPE, [global, trim_all]),
                        handle_metric(S0, Metric, Split)
                end, {ok, State}, Rest).


handle_metric(State, Metric) ->
    handle_metric(State, Metric, {ok, 1}, <<"c">>, {ok, 1}).


handle_metric(State, Metric, [Value]) ->
    handle_metric(State, Metric, statser_util:to_number(Value), <<"c">>, {ok, 1});

% special handling for gauges to differentiate between 'set' and 'modify' values
handle_metric(State, Metric, [Value, <<"g">>]) ->
    handle_metric(State, Metric, parse_gauge_value(Value), <<"g">>, {ok, 1});

handle_metric(State, Metric, [Value, Type]) ->
    handle_metric(State, Metric, statser_util:to_number(Value), Type, {ok, 1});

% special handling for gauges to differentiate between 'set' and 'modify' values
handle_metric(State, Metric, [Value, <<"g">>, <<"@", Sampling/binary>>]) ->
    Sample = statser_util:to_number(Sampling),
    handle_metric(State, Metric, parse_gauge_value(Value), <<"g">>, Sample);

handle_metric(State, Metric, [Value, Type, <<"@", Sampling/binary>>]) ->
    Sample = statser_util:to_number(Sampling),
    handle_metric(State, Metric, statser_util:to_number(Value), Type, Sample);

handle_metric(_State, Metric, Invalid) ->
    lager:debug("invalid metric received: ~p - ~p", [Metric, Invalid]),
    error.


handle_metric(_State, _Metric, error, _Type, _Sample) ->
    lager:debug("invalid value specified"),
    error;

handle_metric(_State, _Metric, _Value, _Type, error) ->
    lager:debug("invalid sample specified"),
    error;

handle_metric(State, Metric, {ok, Value}, <<"c">>, {ok, Sample}) ->
    lager:debug("handle counter: ~p:~w [sample ~w]", [Metric, Value, Sample]),

    Now = statser_util:seconds(),
    Counters0 = State#state.counters,
    Update = fun({_, X}) -> {Now, X + (Value * (1 / Sample))} end,
    Counters = maps:update_with(Metric, Update, {Now, (Value * (1 / Sample))}, Counters0),

    {ok, State#state{counters=Counters}};

handle_metric(State, Metric, {ok, Value}, <<"ms">>, {ok, Sample}) ->
    lager:debug("handle timer: ~p:~w [sample ~w]", [Metric, Value, Sample]),

    Now = statser_util:seconds(),
    Inc = 1 / Sample,
    Timers0 = State#state.timers,
    Update = fun({_, Xs, Cnt}) -> {Now, [Value | Xs], Cnt + Inc} end,
    Timers = maps:update_with(Metric, Update, {Now, [Value], Inc}, Timers0),

    {ok, State#state{timers=Timers}};

handle_metric(State, Metric, {ok, Mod, Value}, <<"g">>, _Sample) ->
    lager:debug("handle gauge: ~p:~w ~w", [Metric, Mod, Value]),

    Now = statser_util:seconds(),
    {Update, Initial} =
    case Mod of
        set -> {fun(_) -> {Now, Value} end, {Now, Value}};
        inc -> {fun({_, X}) -> {Now, X + Value} end, {Now, Value}};
        dec -> {fun({_, X}) -> {Now, X - Value} end, {Now, -Value}}
    end,

    Gauges0 = State#state.gauges,
    Gauges = maps:update_with(Metric, Update, Initial, Gauges0),

    {ok, State#state{gauges=Gauges}};

handle_metric(State, Metric, {ok, Value}, <<"s">>, _Sample) ->
    lager:debug("handle set: ~p:~w", [Metric, Value]),

    Now = statser_util:seconds(),
    Sets0 = State#state.sets,
    Update = fun({_, Xs}) -> {Now, [Value | Xs]} end,
    Sets = maps:update_with(Metric, Update, {Now, [Value]}, Sets0),

    {ok, State#state{sets=Sets}};

handle_metric(_State, _Metric, _Value, Type, _Sample) ->
    lager:debug("invalid metric type specified: ~p [supported: c, ms, g, s]", [Type]),
    error.


parse_gauge_value(<<"+", Rest/binary>>) ->
    parse_gauge_value(inc, Rest);
parse_gauge_value(<<"-", Rest/binary>>) ->
    parse_gauge_value(dec, Rest);
parse_gauge_value(Rest) ->
    parse_gauge_value(set, Rest).


parse_gauge_value(Mod, Value) ->
    case statser_util:to_number(Value) of
        {ok, Val} -> {ok, Mod, Val};
        error -> error
    end.


schedule_flush(#state{config=Config} = State) ->
    % here we calculate the interval for the next scheduled `flush`
    % in order to best match up with the metric's retention
    Now = erlang:system_time(millisecond),
    OffsetToRetention = Now rem Config#udp_config.interval,
    Duration = Config#udp_config.interval - OffsetToRetention + ?BASE_TIMER_CORRECTION,

    Timer = erlang:send_after(Duration, self(), flush),
    State#state{flush_timer=Timer}.


schedule_prune(#state{config=#udp_config{prune_after=Interval}} = State) when is_number(Interval) andalso Interval > 0 ->
    Timer = erlang:send_after(Interval, self(), prune),
    State#state{prune_timer=Timer};

schedule_prune(State) -> State.


%%
%% TESTS
%%

-ifdef(TEST).

handle_packet_test_() ->
    EmptyState = #state{counters=maps:new(),
                        timers=maps:new(),
                        gauges=maps:new(),
                        sets=maps:new(),
                        filters=#metric_filters{}},
    HandleSuccess = fun(P) ->
                            case handle_packet(P, EmptyState) of
                                {ok, _} -> ok;
                                error -> error
                            end
                    end,
    [?_assertEqual(ok, HandleSuccess(<<"foo">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:2">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:2.0|c">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:2.0|c|@0.2">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:2.0|c|@0.2\nfoo:5.2">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:-23|c">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:2.0|ms|@0.1">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:223.3|g">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:223.3|g|@1">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:+12|g">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:-9.2|g">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:23.0|s">>)),
     ?_assertEqual(ok, HandleSuccess(<<"foo:23.0|c:22.0|c">>)),
     ?_assertEqual(error, HandleSuccess(<<"foo:gsh">>)),
     ?_assertEqual(error, HandleSuccess(<<"foo:gsh|c">>)),
     ?_assertEqual(error, HandleSuccess(<<"foo:gsh|c|@0.1">>)),
     ?_assertEqual(error, HandleSuccess(<<"foo:123|t">>)),
     ?_assertEqual(error, HandleSuccess(<<"foo:+|g">>)),
     ?_assertEqual(error, HandleSuccess(<<"foo:123|c|@g">>)),
     ?_assertEqual(error, HandleSuccess(<<"foo:123|c|@1|x">>))
    ].


calculate_timer_test_() ->
    [?_assertEqual({0, 0, 0, 0, 0, 0, 0}, calculate_timer([])),
     ?_assertEqual({0, 100, 100/2, 100/2, 5000.0, 100, 10000}, calculate_timer([100, 0])),
     ?_assertEqual({1, 3, 2.0, 2.0, 2.0, 6, 14}, calculate_timer([1, 3, 2]))
    ].

-endif.
