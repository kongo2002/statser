%%%-------------------------------------------------------------------
%% @doc statser public API
%% @end
%%%-------------------------------------------------------------------

-module(statser_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, prep_stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    % logging
    lager:start(),

    % yaml parsing
    ok = application:ensure_started(yamerl),

    statser_sup:start_link().

%%--------------------------------------------------------------------
prep_stop(_State) ->
    lager:info("preparing statser application shutdown"),
    ok.

%%--------------------------------------------------------------------
stop(_State) ->
    lager:info("stopped statser application"),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
