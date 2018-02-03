-module(test_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-export([start_statser/0,
         stop/1]).

-define(DEFAULT_APPS, [compiler, syntax_tools, goldrush, yamerl, jiffy, elli]).


ensure_default_apps() ->
    lists:foreach(fun(App) -> ok = application:ensure_started(App) end,
                  ?DEFAULT_APPS).


start_statser() ->
    ensure_default_apps(),
    lager:start(),
    lager:set_loglevel(lager_console_backend, emergency),
    ok = application:start(statser),
    ok.


stop(_) ->
    ok = application:stop(statser),
    ok.

-endif.
