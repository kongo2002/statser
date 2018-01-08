-module(statser_health).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-include("statser.hrl").

%% API
-export([start_link/0,
         alive/1,
         subscribe/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(DEFAULT_HEALTH_TIMER_INTERVAL, 30000).

-record(state, {subscribers=[], timer, interval, metrics, services}).

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
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


subscribe(Ref) ->
    gen_server:cast(?MODULE, {subscribe, Ref}).


alive(Name) ->
    % schedule next health heartbeat
    erlang:send_after(?DEFAULT_HEALTH_TIMER_INTERVAL, self(), health),

    % and send an alive report immediately
    Now = erlang:system_time(second),
    gen_server:cast(?MODULE, {alive, Name, Now}),
    ok.


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
init([]) ->
    Interval = 60,

    lager:info("starting health service with update interval of ~w sec", [Interval]),

    State = #state{subscribers=[],
                   metrics=maps:new(),
                   services=maps:new(),
                   interval=Interval * ?MILLIS_PER_SEC},

    {ok, schedule_refresh(State)}.

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
handle_cast({subscribe, Ref}, State) ->
    Subs = State#state.subscribers,
    notify([Ref], State#state.interval, State#state.metrics, State#state.services),
    {noreply, State#state{subscribers=[Ref | Subs]}};

handle_cast({alive, Name, Time}, #state{services=Srv} = State) ->
    Services = maps:put(Name, Time, Srv),
    {noreply, State#state{services=Services}};

handle_cast({metrics, Metrics}, State) ->
    {noreply, State#state{metrics=Metrics}};

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
handle_info(refresh, State) ->
    Subscribers = State#state.subscribers,
    Interval = State#state.interval,
    Metrics = State#state.metrics,
    Services = State#state.services,

    Subs = notify(Subscribers, Interval, Metrics, Services),
    State0 = State#state{subscribers=Subs},

    {noreply, schedule_refresh(State0)};

handle_info(Info, State) ->
    lager:warning("health: unhandled message ~p", [Info]),
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
    lager:info("terminating health service at ~w", [self()]),
    erlang:cancel_timer(Timer, [{async, true}]),
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

schedule_refresh(#state{interval=Interval} = State) ->
    Timer = erlang:send_after(Interval, self(), refresh),
    State#state{timer=Timer}.


notify(Subs, Interval, Metrics, Services) ->
    Now = erlang:system_time(second),
    Stats = lists:map(fun({K, V}) when is_number(V) ->
                               PerSecond = V / Interval,
                               {[{name, K}, {value, PerSecond}, {type, counter}]};
                          ({K, Vs}) when is_list(Vs) ->
                               Avg = statser_calc:safe_average(Vs),
                               {[{name, K}, {value, Avg}, {type, average}]}
                       end, maps:to_list(Metrics)),

    SrvHealth = lists:map(fun({K, V}) ->
                                  Good = (Now - V) < 120,
                                  {[{name, K}, {timestamp, V}, {good, Good}]}
                          end, maps:to_list(Services)),

    Health = [{[{name, <<"health">>}, {good, true}, {timestamp, Now}]},
              {[{name, <<"server">>}, {good, true}, {timestamp, Now}]}],

    Json = jiffy:encode({[{stats, Stats},
                          {interval, Interval},
                          {health, Health ++ SrvHealth}]}),
    Chunk = iolist_to_binary(["data: ", Json, "\n\n"]),

    lists:flatmap(
      fun (Sub) ->
              case elli_request:send_chunk(Sub, Chunk) of
                  ok -> [Sub];
                  {error, closed} -> [];
                  {error, timeout} -> []
              end
      end, Subs).

