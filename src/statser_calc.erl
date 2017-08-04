-module(statser_calc).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([safe_average/1,
         safe_invert/1,
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


safe_invert(null) -> null;
safe_invert(Value) -> math:pow(Value, -1).


safe_square_root(null) -> null;
safe_square_root(Value) -> math:pow(Value, 0.5).


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

-endif.
