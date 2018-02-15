%%%-------------------------------------------------------------------
%% @doc statser metrics handler supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(statser_metrics_sup).

-include("statser.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_handler/1,
         start_handler/2]).

%% Supervisor callbacks
-export([init/1]).


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
-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


-spec start_handler(binary()) -> supervisor:startchild_ret().
start_handler(Path) ->
    supervisor:start_child(?MODULE, [Path]).


-spec start_handler(binary(), whisper_metadata() | none) -> supervisor:startchild_ret().
start_handler(Path, Config) ->
    supervisor:start_child(?MODULE, [Path, Config]).


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
    {ok, {{simple_one_for_one, 60, 3600},
          [{handler,
           {statser_metric_handler, start_link, []},
           temporary, 1000, worker, [statser_metric_handler]}
          ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
