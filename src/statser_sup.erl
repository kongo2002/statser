%%%-------------------------------------------------------------------
%% @doc statser top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(statser_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, initial_listeners/1, start_listener/0]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    NumListeners = application:get_env(listeners, statser, 20),
    Port = application:get_env(port, statser, 3000),

    lager:info("initial load of configuration"),
    ok = statser_config:load_config(),

    lager:info("start listening on port ~w", [Port]),

    % open listening socket
    ListenParams = [{active, false}, binary, {packet, line}],
    {ok, ListenSocket} = gen_tcp:listen(Port, ListenParams),

    % spawn initial pool of listeners
    spawn_link(?MODULE, initial_listeners, [NumListeners]),

    % start actual supervision
    {ok, {{simple_one_for_one, 60, 3600},
          [{socket,
           {statser_listener, start_link, [ListenSocket]},
           temporary, 1000, worker, [statser_listener]}
          ]}}.

%%====================================================================
%% Internal functions
%%====================================================================

start_listener() ->
    supervisor:start_child(?MODULE, []).

initial_listeners(0) -> ok;
initial_listeners(N) ->
    start_listener(),
    initial_listeners(N-1).
