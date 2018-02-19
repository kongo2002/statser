-module(statser_discoverer).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

-define(PERSIST_TIMER_INTERVAL, 60 * ?MILLIS_PER_SEC).
-define(PERSIST_FILE, <<".nodes.json">>).

-type nodes_map() :: #{node() => node_info()}.

-record(state, {
          nodes :: nodes_map(),
          persist_timer :: reference() | undefined
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

    % put ourselves into the nodes as well
    Me = #node_info{node=node(), last_seen=statser_util:seconds(), state=me},
    Nodes = maps:put(node(), Me, maps:new()),

    gen_server:cast(self(), prepare),

    State = #state{nodes=Nodes},
    {ok, State}.

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
handle_cast(prepare, State) ->
    KnownNodes = load_nodes(),

    lager:info("discoverer: loaded ~p configured nodes from persisted '~s' file",
               [maps:size(KnownNodes), ?PERSIST_FILE]),

    % merge loaded nodes with 'current' ones
    Merged = maps:fold(fun(N, Info, Nodes) when N /= node() ->
                               % TODO: trigger node discovery/connection
                               maps:put(N, Info, Nodes);
                          (_N, _Info, Nodes) -> Nodes
                       end, State#state.nodes, KnownNodes),

    {noreply, State#state{nodes=Merged}};

handle_cast({publish, Node}, State) ->
    % publish current node set to everyone but `Node` and ourselves
    maps:fold(fun(K, _Info, _) when K /= Node ->
                      if K /= node() ->
                             lager:debug("publishing ~p to ~p", [Node, K]),
                             {statser_discoverer, K} ! {connect, Node};
                         true -> ok
                      end,
                      lager:debug("publishing ~p to ~p", [K, Node]),
                      {statser_discoverer, Node} ! {connect, K};
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
handle_info({connect, Node}, State) ->
    {_, State0} = try_connect(Node, State),
    {noreply, State0};

handle_info(persist, State) ->
    lager:debug("persisting known nodes now"),
    persist_nodes(State#state.nodes),

    {noreply, State#state{persist_timer=undefined}};

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

schedule_persist_timer(#state{persist_timer=undefined} = State) ->
    Timer = erlang:send_after(?PERSIST_TIMER_INTERVAL, self(), persist),
    State#state{persist_timer=Timer};

schedule_persist_timer(State) ->
    % a timer is already scheduled
    State.


-spec persist_nodes(nodes_map()) -> ok | error.
persist_nodes(Ns) ->
    persist_nodes(Ns, ?PERSIST_FILE).


-spec persist_nodes(nodes_map(), binary()) -> ok | error.
persist_nodes(Ns, File) ->
    % as of now we convert the nodes into a JSON representation
    % no specific reason for this - maybe because we might
    % easily extend this format over time pretty easily
    Nodes = maps:fold(fun(_Node, #node_info{state=me}, Xs) -> Xs;
                         (Node, _Info, Xs) ->
                              [{[{<<"node">>, Node}]} | Xs]
                      end, [], Ns),
    Json = jiffy:encode(Nodes),

    case file:open(File, [write, binary, raw]) of
        {ok, IO} ->
            try
                ok = file:write(IO, Json),
                lager:info("successfully persisted ~p entries to nodes file", [length(Nodes)]),
                ok
            after file:close(IO)
            end;
        _Error -> error
    end.


-spec load_nodes() -> nodes_map().
load_nodes() ->
    load_nodes(?PERSIST_FILE).


-spec load_nodes(binary()) -> nodes_map().
load_nodes(File) ->
    case file:open(File, [read, binary, raw]) of
        {ok, IO} ->
            try load_nodes_from(IO)
            after file:close(IO)
            end;
        {error, enoent} ->
            % file does not exist -> this is totally alright
            maps:new();
        Error ->
            lager:warning("error on loading nodes file: ~p", [Error]),
            maps:new()
    end.


load_nodes_from(IO) ->
    {ok, Data} = file:read(IO, 65536),
    Decoded = jiffy:decode(Data),

    Ns = lists:flatmap(fun({Parts}) ->
                               case proplists:get_value(<<"node">>, Parts, undefined) of
                                   undefined -> [];
                                   Value ->
                                       Node = list_to_atom(binary_to_list(Value)),
                                       [#node_info{node=Node}]
                               end;
                          (_Invalid) -> []
                       end, Decoded),
    maps:from_list(lists:map(fun(#node_info{node=N} = Info) -> {N, Info} end, Ns)).


try_connect(Node, #state{nodes=Ns} = State) ->
    case maps:is_key(Node, Ns) of
        true ->
            lager:info("node ~p is already known/connected", [Node]),
            {true, State};
        false ->
            case net_kernel:connect_node(Node) of
                true ->
                    lager:info("successfully connected to node ~p", [Node]),
                    Info = #node_info{node=Node,
                                      state=connected,
                                      last_seen=statser_util:seconds()},
                    Ns0 = maps:put(Node, Info, Ns),

                    % publish new/updated node
                    gen_server:cast(self(), {publish, Node}),

                    {true, schedule_persist_timer(State#state{nodes=Ns0})};
                false ->
                    lager:warning("connecting to node ~p failed", [Node]),
                    {false, State};
                ignored ->
                    lager:warning("connecting to node ~p failed - local node not alive", [Node]),
                    {ignored, State}
            end
    end.


%%
%% TESTS
%%

-ifdef(TEST).

persist_nodes_test_() ->
    MkNode = fun(N) -> {N, #node_info{node=N}} end,
    Nodes = maps:from_list(lists:map(MkNode, [statser@foo, statser@bar])),

    [?_assertEqual(ok, persist_nodes(maps:new())),
     ?_assertEqual(ok, persist_nodes(Nodes))
    ].

load_nodes_test_() ->
    MkNode = fun(N) -> {N, #node_info{node=N}} end,
    Nodes = maps:from_list(lists:map(MkNode, [statser@foo, statser@bar])),
    PersistAndLoad = fun(Xs) ->
                             ok = persist_nodes(Xs),
                             load_nodes()
                     end,

    [?_assertEqual(maps:new(), load_nodes(<<"/tmp/probably/does/not/exist.json">>)),
     ?_assertEqual(Nodes, PersistAndLoad(Nodes))
    ].

-endif.
