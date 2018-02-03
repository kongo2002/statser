-module(statser_api_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-define(DEFAULT_HOST, "http://localhost:8080").


setup() ->
    ok = test_util:start_statser(),
    inets:start(),
    ok.


teardown(State) ->
    inets:stop(),
    test_util:stop(State),
    ok.


api_metrics_query_non_existing_target_test_() ->
    {
     "test for rendering non existing targets",
     {
      setup,
      fun setup/0, fun teardown/1,
      [
       ?_assertEqual([], metrics_request("does.not.exist")),
       ?_assertEqual([], metrics_request("does.not.*"))
      ]
     }
    }.


api_render_non_existing_target_test_() ->
    {
     "test for rendering non existing targets",
     {
      setup,
      fun setup/0, fun teardown/1,
      [
       ?_assertEqual([], render_request(["does.not.exist"])),
       ?_assertEqual([], render_request(["does.not.exist", "doesnt.exist.either"])),
       ?_assertEqual([], render_request(["does.not.*"]))
      ]
     }
    }.


api_render_functions_of_non_existing_target_test_() ->
    {
     "test for rendering function calls of non existing targets",
     {
      setup,
      fun setup/0, fun teardown/1,
      [
       ?_assertEqual([], render_request(["perSecond(does.not.exist)"])),
       ?_assertEqual([], render_request(["sumSeries(does.not.*)"]))
      ]
     }
    }.


get_request(Path) ->
    Url = ?DEFAULT_HOST ++ Path,
    {ok, Response} = httpc:request(Url),
    Response.


form_post_request(Path, Body) ->
    Url = ?DEFAULT_HOST ++ Path,
    Request = {Url, [], "application/x-www-form-urlencoded", Body},
    {ok, Response} = httpc:request(post, Request, [], []),
    Response.


metrics_request(Path) ->
    Url = "/metrics?query=" ++ Path,
    {{_, 200, "OK"}, _, Body} = get_request(Url),
    jiffy:decode(Body).


render_request(Targets) ->
    Ts0 = lists:map(fun(T) -> "target=" ++ T end, Targets),
    Ts = lists:flatten(lists:join("&", Ts0)),
    {{_, 200, "OK"}, _, Body} = form_post_request("/render", Ts),
    jiffy:decode(Body).


-endif.
