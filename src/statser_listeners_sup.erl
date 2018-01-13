%%%-------------------------------------------------------------------
%% @doc statser listener supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(statser_listeners_sup).

-behaviour(supervisor).

-include("statser.hrl").

%% API
-export([start_link/1,
         initial_listeners/2,
         start_listener/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API functions
%%====================================================================

start_link(#listener_config{supervisor=Supervisor} = Config) ->
    supervisor:start_link({local, Supervisor}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init(Config) ->
    MetricsPort = Config#listener_config.port,
    ChildName = Config#listener_config.child_name,

    lager:info("start listening for metrics on port ~w [type ~p]",
               [MetricsPort, ChildName]),

    % open listening socket
    ListenParams = [{active, false}, binary] ++ Config#listener_config.options,
    {ok, ListenSocket} = gen_tcp:listen(MetricsPort, ListenParams),

    % spawn initial pool of listeners
    spawn_link(?MODULE, initial_listeners, [Config#listener_config.supervisor,
                                            Config#listener_config.listeners]),

    % start actual supervision
    {ok, {{simple_one_for_one, 60, 3600},
          [{socket,
           {ChildName, start_link, [ListenSocket]},
           temporary, 1000, worker, [ChildName]}
          ]}}.

%%====================================================================
%% Internal functions
%%====================================================================

start_listener(Supervisor) ->
    supervisor:start_child(Supervisor, []).

initial_listeners(_Supervisor, 0) -> ok;
initial_listeners(Supervisor, N) ->
    start_listener(Supervisor),
    initial_listeners(Supervisor, N-1).
