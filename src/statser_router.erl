-module(statser_router).

-include("statser.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {timer}).

-define(HEARTBEAT_INTERVAL_SECONDS, 60).

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
    lager:debug("starting router instance at ~p", [self()]),
    statser_health:alive(router),

    {ok, schedule_heartbeat_timer(#state{})}.

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
handle_cast({line, _Path, _Val, _TS} = Line, State) ->
    dispatch(Line),
    {noreply, State};

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
handle_info(health, State) ->
    statser_health:alive(router),
    {noreply, State};

handle_info(heartbeat, State) ->
    Handlers = ets:tab2list(metrics),

    lager:info("checking ~p metric handlers' heartbeats", [length(Handlers)]),

    lists:foreach(fun({_Path, Pid}) ->
                          gen_server:cast(Pid, check_heartbeat)
                  end, Handlers),

    {noreply, schedule_heartbeat_timer(State)};

handle_info(Info, State) ->
    lager:warning("router: unhandled message ~p", [Info]),
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
terminate(_Reason, _State) ->
    lager:info("terminating statser_router"),
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

schedule_heartbeat_timer(State) ->
    Interval = ?HEARTBEAT_INTERVAL_SECONDS * ?MILLIS_PER_SEC,
    Timer = erlang:send_after(Interval, self(), heartbeat),
    State#state{timer=Timer}.


dispatch({line, Path, _, _} = Line) ->
    Target = case ets:lookup(metrics, Path) of
        [] ->
            case statser_metrics_sup:start_handler(Path) of
                {ok, Pid} ->
                    Pid;
                {error, Reason} ->
                    lager:error("failed to start metric handler: ~p", [Reason]),
                    [{Path, Pid}] = ets:lookup(metrics, Path),
                    Pid
            end;
        [{Path, Pid}] ->
            Pid
    end,
    gen_server:cast(Target, Line).
