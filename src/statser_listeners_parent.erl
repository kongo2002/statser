-module(statser_listeners_parent).

-behaviour(gen_server).

-include("statser.hrl").

%% API
-export([start_link/1,
         start_listener/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {config, socket}).

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
start_link(#listener_config{supervisor=Supervisor} = Config) ->
    gen_server:start_link({local, Supervisor}, ?MODULE, Config, []).


start_listener(Supervisor) ->
    gen_server:cast(Supervisor, start).

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
init(Config) ->
    Type = Config#listener_config.child_name,
    lager:debug("initializing parent for listeners of type ~p", [Type]),

    gen_server:cast(self(), prepare),

    {ok, #state{config=Config, socket=none}}.

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
handle_cast(prepare, #state{config=Config} = State) ->
    SelfType = Config#listener_config.supervisor,
    Port = Config#listener_config.port,
    ChildName = Config#listener_config.child_name,

    lager:info("start listening for metrics on port ~w [type ~p]",
               [Port, ChildName]),

    % open listening socket
    ListenParams = [{active, false}, binary] ++ Config#listener_config.options,
    {ok, Socket} = gen_tcp:listen(Port, ListenParams),

    % start initial batch of listeners
    lists:foreach(fun(_) -> start_listener(SelfType) end,
                  lists:seq(1, Config#listener_config.listeners)),

    {noreply, State#state{socket=Socket}};

handle_cast(start, #state{config=Config} = State) ->
    Module = Config#listener_config.child_name,
    {ok, _Pid} = gen_server:start_link(Module, State#state.socket, []),

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
handle_info(Info, #state{config=Config} = State) ->
    Type = Config#listener_config.child_name,
    lager:warning("unexpected message in listener parent for ~p: ~p", [Type, Info]),
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
terminate(_Reason, #state{socket=none}) ->
    ok;

terminate(_Reason, #state{socket=Socket}) ->
    gen_tcp:close(Socket),
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
