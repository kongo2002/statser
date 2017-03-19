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
    % logging
    lager:start(),

    % yaml parsing
    ok = application:ensure_started(yamerl),

    statser_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    lager:info("stopping statser application"),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
