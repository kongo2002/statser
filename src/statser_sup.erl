%%%-------------------------------------------------------------------
%% @doc statser top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(statser_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args),
        {Id, {Mod, start_link, Args},
         permanent, 5000, Type, [Mod]}).

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

    ApiPort = application:get_env(api_port, statser, 8080),

    lager:info("start listening for API requests on port ~w", [ApiPort]),

    {ok, {{one_for_one, 5, 10},
          [?CHILD(listeners, statser_listeners_sup, supervisor, []),
           ?CHILD(metrics, statser_metrics_sup, supervisor, []),
           ?CHILD(router, statser_router, worker, []),
           ?CHILD(instrumentation, statser_instrumentation, worker, []),
           % TODO: udp - depending on config
           ?CHILD(udp_listener, statser_listener_udp, worker, [8125]),
           % rate limiters
           ?CHILD(create_limiter, statser_rate_limiter, worker, [create_limiter, <<"creates">>]),
           ?CHILD(update_limiter, statser_rate_limiter, worker, [update_limiter, <<"updates">>]),
           % API
           ?CHILD(api, elli, worker, [[{callback, statser_api}, {port, ApiPort}]])
          ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
