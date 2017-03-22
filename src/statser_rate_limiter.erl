-module(statser_rate_limiter).

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {limit, remaining, timer, name}).

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
start_link(Type, Name) ->
    gen_server:start_link({local, Type}, ?MODULE, [Type, Name], []).

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
init([Type, Name]) ->
    % TODO: get from config based on `Type`
    Limit = 50,

    lager:info("starting rate limiter [~w] with limit ~w/sec", [Type, Limit]),

    % the rate limiter is per-second based
    % that's why we will refill the capacity once every second
    {ok, Timer} = timer:send_interval(1000, refill),

    {ok, #state{limit=Limit, remaining=Limit, timer=Timer, name=Name}}.

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
    case drain(State) of
        {ok, NewState} ->
            To ! Reply,
            {noreply, NewState};
        none ->
            {noreply, State}
    end;
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
handle_info(refill, State) ->
    Limit = State#state.limit,
    NewState = State#state{remaining=Limit},
    {noreply, NewState};
handle_info({drain, Reply, To}, State) ->
    case drain(State) of
        {ok, NewState} ->
            To ! Reply,
            {noreply, NewState};
        none ->
            {noreply, State}
    end;
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
terminate(_Reason, State) ->
    lager:info("terminating rate limiter at ~w", [self()]),
    timer:cancel(State#state.timer),
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

drain(#state{remaining=Rem} = State) when Rem > 0 ->
    {ok, State#state{remaining=Rem-1}};
drain(#state{limit=Limit, name=Name}) ->
    lager:warning("request is dropped due to exhausted rate limiter [~w/sec]", [Limit]),
    statser_instrumentation:increment(<<Name/binary, "-dropped">>),
    none.
