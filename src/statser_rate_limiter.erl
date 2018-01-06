-module(statser_rate_limiter).

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(MILLIS_PER_SEC, 1000).

-record(state, {limit, remaining, interval, timer, name, pending}).

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
start_link(Type, Name, Limit) ->
    gen_server:start_link({local, Type}, ?MODULE, [Type, Name, Limit], []).

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
init([Type, Name, LimitPerSec]) ->
    lager:info("starting rate limiter [~w] with limit ~w/sec", [Type, LimitPerSec]),

    % we want to distribute the `refill` as evenly as possible
    % that's why we calculate the rate limit to a minimum interval of 100 ms
    IntervalInMillis = max(?MILLIS_PER_SEC div LimitPerSec, 100),
    LimitPerInterval = LimitPerSec div (?MILLIS_PER_SEC div IntervalInMillis),

    State = #state{limit=LimitPerInterval,
                   remaining=LimitPerInterval,
                   interval=IntervalInMillis,
                   name=Name,
                   pending=queue:new()},

    alive(Name),
    {ok, schedule_refill(State)}.

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
handle_call(drain, _From, State) ->
    case drain(State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        none ->
            {reply, none, State}
    end;

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
handle_cast({drain, Reply, To}, State) ->
    NewState = drain(State, Reply, To),
    {noreply, NewState};

handle_cast(_Cast, State) ->
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
handle_info(refill, #state{limit=Limit, name=Name} = State) ->
    % XXX: in 'dev-mode' only? or keep track of queue-size?
    statser_instrumentation:record(<<Name/binary, "-cache-size">>,
                                   queue:len(State#state.pending)),

    {Pending, Remaining} = drain_queue(State#state.pending, Limit),
    NewState = State#state{remaining=Remaining, pending=Pending},
    {noreply, schedule_refill(NewState)};

handle_info({drain, Reply, To}, State) ->
    case drain(State) of
        {ok, NewState} ->
            To ! Reply,
            {noreply, NewState};
        none ->
            {noreply, State}
    end;

handle_info(health, #state{name=Name} = State) ->
    alive(Name),
    {noreply, State};

handle_info(Info, State) ->
    lager:warning("rate-limiter: unhandled message ~p", [Info]),
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
terminate(_Reason, State) ->
    lager:info("terminating rate limiter at ~w", [self()]),
    erlang:cancel_timer(State#state.timer, [{async, true}]),
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
alive(Name) ->
    statser_health:alive(<<Name/binary, "-rate-limiter">>).


schedule_refill(#state{interval=Interval} = State) ->
    Timer = erlang:send_after(Interval, self(), refill),
    State#state{timer=Timer}.


drain(#state{remaining=Rem} = State) when Rem > 0 ->
    {ok, State#state{remaining=Rem-1}};

drain(#state{name=Name}) ->
    statser_instrumentation:increment(<<Name/binary, "-dropped">>),
    none.


drain(#state{remaining=Rem} = State, Reply, To) when Rem > 0 ->
    To ! Reply,
    State#state{remaining=Rem-1};

drain(#state{name=Name} = State, Reply, To) ->
    Item = {Reply, To},

    % XXX: not sure about this one yet - it has O(n) complexity
    case queue:member(Item, State#state.pending) of
        true ->
            State;
        _ ->
            statser_instrumentation:increment(<<Name/binary, "-dropped">>),

            Pending = queue:in(Item, State#state.pending),
            State#state{pending=Pending}
    end.


drain_queue(Queue, 0) -> {Queue, 0};
drain_queue(Queue, Remaining) ->
    case queue:out(Queue) of
        {empty, _Q} -> {Queue, Remaining};
        {{value, {Reply, To}}, Q} ->
            To ! Reply,
            drain_queue(Q, Remaining-1)
    end.
