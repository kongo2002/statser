-module(statser_calc).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([safe_average/1,
         safe_invert/1,
         safe_div/2,
         safe_max/1,
         safe_min/1,
         safe_square_root/1]).


safe_average(Values) ->
    safe_average(Values, 1, 0.0).

safe_average([], _Cnt, Avg) -> Avg;
safe_average([{_TS, null} | Vs], Cnt, Avg) ->
    safe_average(Vs, Cnt, Avg);
safe_average([{_TS, Val} | Vs], Cnt, Avg) ->
    NewAvg = Avg + (Val - Avg) / Cnt,
    safe_average(Vs, Cnt + 1, NewAvg);
safe_average([null | Vs], Cnt, Avg) ->
    safe_average(Vs, Cnt, Avg);
safe_average([Val | Vs], Cnt, Avg) ->
    NewAvg = Avg + (Val - Avg) / Cnt,
    safe_average(Vs, Cnt + 1, NewAvg).


safe_max(Vs) ->
    safe_max(Vs, 0.0).

safe_max([], Max) -> Max;
safe_max([{_TS, null} | Vs], Max) ->
    safe_max(Vs, Max);
safe_max([{_TS, Val} | Vs], Max) ->
    safe_max(Vs, max(Val, Max));
safe_max([null | Vs], Max) ->
    safe_max(Vs, Max);
safe_max([Val | Vs], Max) ->
    safe_max(Vs, max(Val, Max)).


safe_min(Vs) ->
    safe_min(Vs, null).

safe_min([], Min) -> Min;
safe_min([{_TS, null} | Vs], Min) ->
    safe_min(Vs, Min);
safe_min([{_TS, Val} | Vs], Min) ->
    safe_min(Vs, min(Val, Min));
safe_min([null | Vs], Min) ->
    safe_min(Vs, Min);
safe_min([Val | Vs], Min) ->
    safe_min(Vs, min(Val, Min)).


safe_invert(null) -> null;
safe_invert(Value) -> math:pow(Value, -1).


safe_square_root(null) -> null;
safe_square_root(Value) -> math:pow(Value, 0.5).


safe_div(_A, 0) -> null;
safe_div(_A, null) -> null;
safe_div({TS, _A}, {_, 0}) -> {TS, null};
safe_div({TS, _A}, {_, null}) -> {TS, null};
safe_div(null, _B) -> null;
safe_div({TS, null}, _B) -> {TS, null};
safe_div({TS, A}, {_, B}) -> {TS, A / B};
safe_div(A, B) -> A / B.


%%
%% TESTS
%%

-ifdef(TEST).

safe_average_test_() ->
    [?_assertEqual(4.0, safe_average([4])),
     ?_assertEqual(3.0, safe_average([2, 4])),
     ?_assertEqual(3.0, safe_average([2, 4, 4, 2])),
     ?_assertEqual(0.0, safe_average([])),
     ?_assertEqual(0.0, safe_average([null])),
     ?_assertEqual(3.0, safe_average([2, 4, null])),
     ?_assertEqual(4.0, safe_average([null, 4, null])),
     ?_assertEqual(3.0, safe_average([{0, 2}, {0, 4}, {0, null}])),
     ?_assertEqual(4.0, safe_average([{0, null}, {0, 4}, {0, null}]))
    ].

safe_div_test_() ->
    [?_assertEqual(5.0, safe_div(10, 2)),
     ?_assertEqual(0.0, safe_div(0, 2)),
     ?_assertEqual(null, safe_div(123.1, 0)),
     ?_assertEqual(null, safe_div(null, 2)),
     ?_assertEqual(null, safe_div(3, null)),
     ?_assertEqual({100, 5.0}, safe_div({100, 10}, {100, 2})),
     ?_assertEqual({100, null}, safe_div({100, null}, {100, 2})),
     ?_assertEqual({100, null}, safe_div({100, 20.0}, {100, null}))
    ].

-endif.
