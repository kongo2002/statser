-module(test_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-export([start_statser/0,
         start_statser/1,
         stop/1]).

-export([send_metric/2,
         send_metric/3,
         with_tempfile/1]).

-define(DEFAULT_APPS, [compiler, syntax_tools, goldrush, yamerl, jiffy, elli]).

-record(test_ctx, {
          tempdir :: nonempty_string()
         }).

ensure_default_apps() ->
    lists:foreach(fun(App) -> ok = application:ensure_started(App) end,
                  ?DEFAULT_APPS).


start_statser() ->
    start_statser(emergency).

start_statser(LogLevel) ->
    ensure_default_apps(),
    lager:start(),
    lager:set_loglevel(lager_console_backend, LogLevel),
    TempDir = tempdir(),
    ok = application:start(statser),
    application:set_env(statser, data_dir, list_to_binary(TempDir)),
    #test_ctx{tempdir=TempDir}.


stop(#test_ctx{tempdir=[]}) ->
    ok = application:stop(statser);

stop(#test_ctx{tempdir=Dir}) ->
    ok = application:stop(statser),
    os:cmd("rm -Rf " ++ Dir),
    ok.


send_metric(Metric, Value) ->
    send_metric(Metric, Value, statser_util:seconds()).

send_metric(Metric, Value, TS) ->
    Host = "127.0.0.1",
    Port = 2003,
    Line = Metric ++ " " ++ integer_to_list(Value) ++ " " ++ integer_to_list(TS) ++ "\n",
    {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {packet, 0}]),
    ok = gen_tcp:send(Sock, Line),
    ok = gen_tcp:close(Sock).


tempfile() ->
    string:trim(os:cmd("mktemp"), trailing, "\r\n").


tempdir() ->
    string:trim(os:cmd("mktemp -d"), trailing, "\r\n").


with_tempfile(Fun) ->
    TempFile = tempfile(),
    try Fun(TempFile)
        after file:delete(TempFile)
    end.


-endif.
