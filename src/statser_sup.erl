%%%-------------------------------------------------------------------
%% @doc statser top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(statser_sup).

-behaviour(supervisor).

-include("statser.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(WORKER(Id, Mod, Args),
        {Id, {Mod, start_link, Args},
         permanent, 5000, worker, [Mod]}).

-define(SUP(Id, Mod, Args),
        {Id, {Mod, start_link, Args},
         permanent, infinity, supervisor, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    lager:info("initial load of configuration"),
    ok = statser_config:load_config(),

    % initialize ets table
    % TODO: does this belong in here?
    % TODO: investigate into 'read_concurrency'
    ets:new(metrics, [set, named_table, public]),

    % TODO: configurable API port
    ApiPort = application:get_env(statser, api_port, 8080),

    Limits = statser_config:get_rate_limits(),
    UdpConfig = statser_config:get_udp_config(),

    UdpChild =
    case statser_config:udp_is_enabled(UdpConfig) of
        true -> [?WORKER(udp_listener, statser_listener_udp, [UdpConfig])];
        false -> []
    end,

    lager:info("start listening for API requests on port ~w", [ApiPort]),

    Children = [?SUP(listeners, statser_listeners_sup, []),
                ?SUP(metrics, statser_metrics_sup, []),
                % health endpoint
                ?WORKER(health, statser_health, []),
                % metrics router
                ?WORKER(router, statser_router, []),
                % instrumentation service
                ?WORKER(instrumentation, statser_instrumentation, []),
                % metrics watcher
                ?WORKER(metrics_watcher, statser_metrics_watcher, []),
                % rate limiters
                ?WORKER(create_limiter, statser_rate_limiter,
                       [create_limiter, <<"creates">>, Limits#rate_limit_config.creates_per_sec]),
                ?WORKER(update_limiter, statser_rate_limiter,
                       [update_limiter, <<"updates">>, Limits#rate_limit_config.updates_per_sec]),
                % API
                ?WORKER(api, elli, [[{callback, statser_api}, {port, ApiPort}]])
               ],

    {ok, {{one_for_one, 5, 10}, Children ++ UdpChild}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
