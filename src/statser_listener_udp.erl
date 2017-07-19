-module(statser_listener_udp).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

-record(state, {port, socket, counters, timers, gauges, sets, timer}).

-define(DELIMITER_PIPE, <<"|">>).
-define(DELIMITER_COLON, <<":">>).

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
start_link(Port) ->
    gen_server:start_link(?MODULE, Port, []).

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
init(Port) ->
    lager:info("starting UDP listener at port ~w", [Port]),

    gen_server:cast(self(), accept),
    {ok, #state{port=Port,
                counters=maps:new(),
                timers=maps:new(),
                gauges=maps:new(),
                sets=maps:new()}}.

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
handle_cast(accept, #state{port=Port} = State) ->
    Self = self(),
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    lager:info("UDP listener opened port at ~w [~p]", [Port, Socket]),

    % TODO: configurable interval
    Interval = 10 * 1000,

    % TODO: maybe we want to synchronize the flush interval with
    % the wall clock time matching with the graphite aggregation
    {ok, Timer} = timer:send_interval(Interval, flush),

    % TODO: handle exit/failure
    spawn_link(?MODULE, listen, [Self, Socket]),
    {noreply, State#state{socket=Socket, timer=Timer}};

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

handle_info(flush, State) ->
    Start = erlang:monotonic_time(millisecond),
    Now = erlang:system_time(second),

    % TODO: configurable if values should be reset or removed

    % flush counters
    Counters = maps:map(fun(K, V) ->
                                publish(<<"counters.", K/binary>>, V, Now),
                                0
                        end, State#state.counters),

    % flush gauges
    Gauges = maps:map(fun(K, V) ->
                                publish(<<"gauges.", K/binary>>, V, Now),
                                0
                        end, State#state.gauges),

    % TODO: expose duration as instrumentation/metric
    Duration = erlang:monotonic_time(millisecond) - Start,
    lager:debug("flush duration: ~w ms", [Duration]),

    {noreply, State#state{counters=Counters,
                          gauges=Gauges}};

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
terminate(_Reason, #state{timer=Timer}) ->
    lager:info("terminating UDP listener service at ~w", [self()]),
    timer:cancel(Timer),
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
    gen_server:cast(statser_router, {line, Metric, Value, TS}).


calculate_timer([], _Count) -> null;
calculate_timer(Values, Count) ->
    Sorted = lists:sort(Values),
    Min = hd(Sorted),
    Max = lists:last(Sorted),
    {Sums, SquaredSums, Sum, SquaredSum} = cumulative(Sorted),
    {Min, Max, Sums, SquaredSums, Sum, SquaredSum}.


cumulative([V | Vs]) ->
    cumulative([V], [V * V], Vs).

cumulative([A | _] = As, [B | _] = Bs, []) ->
    {lists:reverse(As), lists:reverse(Bs), A, B};
cumulative([A | _] = As, [B | _] = Bs, [X | Xs]) ->
    cumulative([A + X | As], [B + X * X | Bs], Xs).


handle(Packet, State) ->
    lager:debug("handle UDP packet: ~p", [Packet]),

    Result = case binary:split(Packet, ?DELIMITER_COLON) of
                 [Metric, Rest] ->
                     Split = binary:split(Rest, ?DELIMITER_PIPE, [global, trim_all]),
                     handle_metric(State, Metric, Split);
                 _ ->
                     % TODO: check for valid metrics name
                     handle_metric(State, Packet)
             end,
    case Result of
        {ok, Updated} ->
            statser_instrumentation:increment(<<"udp.handled-values">>),
            Updated;
        error ->
            statser_instrumentation:increment(<<"udp.invalid-metrics">>),
            State
    end.


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

    Counters0 = State#state.counters,
    Update = fun(X) -> X + (Value * (1 / Sample)) end,
    Counters = maps:update_with(Metric, Update, (Value * (1 / Sample)), Counters0),

    {ok, State#state{counters=Counters}};

handle_metric(State, Metric, {ok, Value}, <<"ms">>, {ok, Sample}) ->
    lager:debug("handle timer: ~p:~w [sample ~w]", [Metric, Value, Sample]),

    Inc = 1 / Sample,
    Timers0 = State#state.timers,
    Update = fun({Xs, Cnt}) -> {[Value | Xs], Cnt + Inc} end,
    Timers = maps:update_with(Metric, Update, {[Value], Inc}, Timers0),

    {ok, State#state{timers=Timers}};

handle_metric(State, Metric, {ok, Mod, Value}, <<"g">>, _Sample) ->
    lager:debug("handle gauge: ~p:~w ~w", [Metric, Mod, Value]),

    {Update, Initial} =
    case Mod of
        set -> {fun(_) -> Value end, Value};
        inc -> {fun(X) -> X + Value end, Value};
        dec -> {fun(X) -> X - Value end, -Value}
    end,

    Gauges0 = State#state.gauges,
    Gauges = maps:update_with(Metric, Update, Initial, Gauges0),

    {ok, State#state{gauges=Gauges}};

handle_metric(State, Metric, {ok, Value}, <<"s">>, _Sample) ->
    lager:debug("handle set: ~p:~w", [Metric, Value]),

    Sets0 = State#state.sets,
    Update = fun(X) -> sets:add_element(Value, X) end,
    Sets = maps:update_with(Metric, Update, sets:from_list([Value]), Sets0),

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


%%
%% TESTS
%%

-ifdef(TEST).

calculate_timer_test_() ->
    [?_assertEqual(null, calculate_timer([], 0)),
     ?_assertEqual({0, 100, [0, 100], [0, 10000], 100, 10000}, calculate_timer([100, 0], 2)),
     ?_assertEqual({1, 3, [1, 3, 6], [1, 5, 14], 6, 14}, calculate_timer([1, 3, 2], 3))
    ].

-endif.
