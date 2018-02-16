-module(statser_discoverer).

-behaviour(gen_server).

-include("statser.hrl").

%% API
-export([start_link/0]).

-export([connect/1,
         get_nodes/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          nodes :: #{node() => node_info()}
         }).

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


-spec connect(node()) -> boolean() | ignored.
connect(Node) ->
    gen_server:call(?MODULE, {connect, Node}).


-spec get_nodes() -> [node_info()].
get_nodes() ->
    gen_server:call(?MODULE, get_nodes).

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
    lager:debug("starting discoverer at ~p", [self()]),
    {ok, #state{nodes=maps:new()}}.

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
handle_call({connect, Node}, _From, State) ->
    lager:info("trying to connect to node ~p", [Node]),

    {Reply, State0} = try_connect(Node, State),
    {reply, Reply, State0};

handle_call(get_nodes, _From, State) ->
    Nodes = maps:values(State#state.nodes),
    {reply, Nodes, State};

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
handle_cast({publish, Node}, State) ->
    % publish current node set to everyone but `Node` and ourselves
    maps:fold(fun(K, _Info, _) when K /= Node andalso K /= node() ->
                      {K, statser_discoverer} ! node_update;
                 (_, _, _) -> ok
              end, ok, State#state.nodes),

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
handle_info(Info, State) ->
    lager:warning("discoverer: unhandled message ~p", [Info]),
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

try_connect(Node, #state{nodes=Ns} = State) ->
    case maps:is_key(Node, Ns) of
        true ->
            lager:info("node ~p is already known/connected", [Node]),
            {true, State};
        false ->
            case net_kernel:connect_node(Node) of
                true ->
                    lager:info("successfully connected to node ~p", [Node]),
                    Info = #node_info{node=Node, last_seen=statser_util:seconds()},
                    Ns0 = maps:put(Node, Info, Ns),

                    % publish new/updated node
                    gen_server:cast(self(), {publish, Node}),

                    {true, State#state{nodes=Ns0}};
                false ->
                    lager:warning("connecting to node ~p failed", [Node]),
                    {false, State};
                ignored ->
                    lager:warning("connecting to node ~p failed - local node not alive", [Node]),
                    {ignored, State}
            end
    end.
