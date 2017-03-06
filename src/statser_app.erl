%%%-------------------------------------------------------------------
%% @doc statser public API
%% @end
%%%-------------------------------------------------------------------

-module(statser_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    lager:start(),

    % FIXME: this doesn't belong in here
    lager:set_loglevel(lager_console_backend, debug),

    % initialize ets table
    % TODO: does this belong in here?
    % TODO: investigate into 'read_concurrency'
    ets:new(metrics, [set, named_table, public]),

    statser_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
