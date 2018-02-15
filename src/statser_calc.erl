-module(statser_calc).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([median/1,
         percentile/2,
         percentile/3,
         safe_average/1,
         safe_length/1,
         safe_invert/1,
         safe_div/2,
         safe_max/1,
         safe_min/1,
         safe_pow/1,
         safe_range/1,
         safe_stddev/1,
         safe_sum/1,
         safe_diff/1,
         safe_substract/2,
         safe_square_root/1]).


-type variadic_metric() :: metric_tuple() | metric_value().
-type variadic_metrics() :: [variadic_metric()].


-spec safe_average(variadic_metrics()) -> number().
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


-spec safe_length(variadic_metrics()) -> integer().
safe_length(Vs) ->
    safe_length(Vs, 0).

safe_length([], Len) -> Len;
safe_length([null | Xs], Len) ->
    safe_length(Xs, Len);
safe_length([{_TS, null} | Xs], Len) ->
    safe_length(Xs, Len);
safe_length([_ | Xs], Len) ->
    safe_length(Xs, Len + 1).


-spec safe_max(variadic_metrics()) -> metric_value().
safe_max(Vs) ->
    safe_max(Vs, null).

safe_max([], Max) -> Max;
safe_max([{_TS, V} | Vs], null) ->
    safe_max(Vs, V);
safe_max([{_TS, null} | Vs], Max) ->
    safe_max(Vs, Max);
safe_max([{_TS, Val} | Vs], Max) ->
    safe_max(Vs, max(Val, Max));
safe_max([null | Vs], Max) ->
    safe_max(Vs, Max);
safe_max([Val | Vs], null) ->
    safe_max(Vs, Val);
safe_max([Val | Vs], Max) ->
    safe_max(Vs, max(Val, Max)).


-spec safe_pow(variadic_metrics()) -> metric_value().
safe_pow([]) -> null;
safe_pow([null | _Vs]) -> null;
safe_pow([{_TS, null} | _Vs]) -> null;
safe_pow([{_TS, V} | Vs]) ->
    safe_pow(Vs, V);
safe_pow([V | Vs]) ->
    safe_pow(Vs, V).

safe_pow([], Current) -> Current;
safe_pow([{_TS, null} | _Vs], _Current) -> null;
safe_pow([null | _Vs], _Current) -> null;
safe_pow([{_TS, Val} | Vs], Current) ->
    safe_pow(Vs, math:pow(Current, Val));
safe_pow([Val | Vs], Current) ->
    safe_pow(Vs, math:pow(Current, Val)).


-spec safe_min(variadic_metrics()) -> metric_value().
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


-spec safe_max_compare(metric_value(), metric_value()) -> metric_value().
safe_max_compare(A, null) -> A;
safe_max_compare(null, B) -> B;
safe_max_compare(A, B) -> max(A, B).


-spec safe_range(variadic_metrics()) -> metric_value().
safe_range(Vs) ->
    safe_range(Vs, {null, null}).

safe_range([], {Min, Max}) ->
    safe_substract(Max, Min);
safe_range([{_TS, null} | Vs], MinMax) ->
    safe_range(Vs, MinMax);
safe_range([{_TS, Val} | Vs], {Min, Max}) ->
    MinMax = {min(Val, Min), safe_max_compare(Val, Max)},
    safe_range(Vs, MinMax);
safe_range([null | Vs], MinMax) ->
    safe_range(Vs, MinMax);
safe_range([Val | Vs], {Min, Max}) ->
    MinMax = {min(Val, Min), safe_max_compare(Val, Max)},
    safe_range(Vs, MinMax).


-spec safe_diff(variadic_metrics()) -> number().
safe_diff([]) -> 0;
safe_diff([{_TS, Value} | Vs]) -> safe_diff(Vs, Value);
safe_diff([Value | Vs]) -> safe_diff(Vs, Value).

safe_diff([], null) -> 0;
safe_diff([], Acc) -> Acc;
safe_diff([null | Vs], Acc) ->
    safe_diff(Vs, Acc);
safe_diff([{_TS, null} | Vs], Acc) ->
    safe_diff(Vs, Acc);
safe_diff([{_TS, Value} | Vs], null) ->
    safe_diff(Vs, Value);
safe_diff([{_TS, Value} | Vs], Acc) ->
    safe_diff(Vs, Acc - Value);
safe_diff([Value | Vs], null) ->
    safe_diff(Vs, Value);
safe_diff([Value | Vs], Acc) ->
    safe_diff(Vs, Acc - Value).


-spec safe_substract(metric_value(), metric_value()) -> metric_value().
safe_substract(_A, null) -> null;
safe_substract(null, _B) -> null;
safe_substract(A, B) -> A - B.


-spec safe_invert(metric_value()) -> metric_value().
safe_invert(null) -> null;
safe_invert(Value) -> math:pow(Value, -1).


-spec safe_stddev(variadic_metrics()) -> metric_value().
safe_stddev(Vs) ->
    safe_stddev(Vs, safe_average(Vs), 0.0, 0).

safe_stddev([], _Avg, Sum, Len) when Len > 0 ->
    math:sqrt(Sum / Len);
safe_stddev([], _Avg, _Sum, _Len) -> null;
safe_stddev([{_TS, null} | Vs], Avg, Sum, Len) ->
    safe_stddev(Vs, Avg, Sum, Len);
safe_stddev([{_TS, V} | Vs], Avg, Sum, Len) ->
    Dev = math:pow(V - Avg, 2),
    safe_stddev(Vs, Avg, Sum + Dev, Len + 1);
safe_stddev([null | Vs], Avg, Sum, Len) ->
    safe_stddev(Vs, Avg, Sum, Len);
safe_stddev([V | Vs], Avg, Sum, Len) ->
    Dev = math:pow(V - Avg, 2),
    safe_stddev(Vs, Avg, Sum + Dev, Len + 1).


-spec safe_sum(variadic_metrics()) -> number().
safe_sum(Vs) ->
    safe_sum(Vs, 0).

safe_sum([], Acc) -> Acc;
safe_sum([null | Vs], Acc) ->
    safe_sum(Vs, Acc);
safe_sum([{_TS, null} | Vs], Acc) ->
    safe_sum(Vs, Acc);
safe_sum([{_TS, Value} | Vs], Acc) ->
    safe_sum(Vs, Acc + Value);
safe_sum([Value | Vs], Acc) ->
    safe_sum(Vs, Acc + Value).


-spec safe_square_root(metric_value()) -> metric_value().
safe_square_root(null) -> null;
safe_square_root(Value) -> math:pow(Value, 0.5).


-spec safe_div(variadic_metric(), variadic_metric()) -> variadic_metric().
safe_div(_A, 0) -> null;
safe_div(_A, null) -> null;
safe_div({TS, _A}, {_, 0}) -> {TS, null};
safe_div({TS, _A}, {_, null}) -> {TS, null};
safe_div(null, _B) -> null;
safe_div({TS, null}, _B) -> {TS, null};
safe_div({TS, A}, {_, B}) -> {TS, A / B};
safe_div({TS, A}, B) -> {TS, A / B};
safe_div(A, B) -> A / B.


-spec sort_non_null(variadic_metrics()) -> {variadic_metrics(), integer()}.
sort_non_null(Values) ->
    sort_non_null(Values, [], 0).

sort_non_null([], Acc, Len) ->
    {lists:sort(Acc), Len};
sort_non_null([null | Vs], Acc, Len) ->
    sort_non_null(Vs, Acc, Len);
sort_non_null([{_TS, null} | Vs], Acc, Len) ->
    sort_non_null(Vs, Acc, Len);
sort_non_null([{_TS, Val} | Vs], Acc, Len) ->
    sort_non_null(Vs, [Val | Acc], Len + 1);
sort_non_null([Val | Vs], Acc, Len) ->
    sort_non_null(Vs, [Val | Acc], Len + 1).


-spec median([metric_value()]) -> metric_value().
median(Values) ->
    percentile(Values, 50, true).


-spec percentile([metric_value()], integer()) -> metric_value().
percentile(Values, N) ->
    percentile(Values, N, false).

-spec percentile([metric_value()], integer(), boolean()) -> metric_value().
percentile(Values, N, Interpolate) ->
    {Sorted, Len} = sort_non_null(Values),
    FractionalRank = (N / 100.0) * (Len + 1),
    Rank0 = statser_util:floor(FractionalRank),
    RankFraction = FractionalRank - Rank0,

    Rank =
    if Interpolate == true -> Rank0;
       true -> Rank0 + statser_util:ceiling(RankFraction)
    end,

    Percentile =
    if Len == 0 -> null;
       Rank == 0 -> hd(Sorted);
       Rank > Len -> lists:nth(Len, Sorted);
       true -> lists:nth(Rank, Sorted)
    end,

    if Interpolate == true andalso Len > Rank ->
           NextValue = lists:nth(Rank + 1, Sorted),
           Percentile + RankFraction * (NextValue - Percentile);
       true ->
           Percentile
    end.


%%
%% TESTS
%%

-ifdef(TEST).

sort_non_null_test_() ->
    [?_assertEqual({[], 0}, sort_non_null([])),
     ?_assertEqual({[], 0}, sort_non_null([null, null])),
     ?_assertEqual({[1], 1}, sort_non_null([1])),
     ?_assertEqual({[1, 2], 2}, sort_non_null([1, null, 2])),
     ?_assertEqual({[1, 2], 2}, sort_non_null([2, null, 1]))
    ].

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

safe_pow_test_() ->
    [?_assertEqual(null, safe_pow([])),
     ?_assertEqual(null, safe_pow([1, 2, 4, 3, null, 2])),
     ?_assertEqual(1, safe_pow([1])),
     ?_assertEqual(math:pow(1, 2), safe_pow([1, 2])),
     ?_assertEqual(math:pow(1, 2), safe_pow([{100, 1}, {110, 2}]))
    ].

safe_range_test_() ->
    [?_assertEqual(null, safe_range([null, null])),
     ?_assertEqual(null, safe_range([null])),
     ?_assertEqual(null, safe_range([])),
     ?_assertEqual(1, safe_range([1, 2])),
     ?_assertEqual(2, safe_range([3, 1])),
     ?_assertEqual(2, safe_range([3, null, 1])),
     ?_assertEqual(5, safe_range([3, null, -1, 2, 1, 3, 4, 1])),
     ?_assertEqual(0, safe_range([1]))
    ].

safe_diff_test_() ->
    [?_assertEqual(0, safe_diff([])),
     ?_assertEqual(1, safe_diff([1])),
     ?_assertEqual(0, safe_diff([1, 1])),
     ?_assertEqual(-5, safe_diff([1, 1, 2, 3])),
     ?_assertEqual(-5, safe_diff([null, 1, 1, 2, null, 3, null])),
     ?_assertEqual(0, safe_diff([null, null, null]))
    ].

safe_stddev_test_() ->
    [?_assertEqual(null, safe_stddev([])),
     ?_assertEqual(null, safe_stddev([null])),
     ?_assertEqual(0.0, safe_stddev([null, 1])),
     ?_assertEqual(math:sqrt(8/3), safe_stddev([2, 4, null, 6]))
    ].

safe_length_test_() ->
    [?_assertEqual(0, safe_length([])),
     ?_assertEqual(0, safe_length([null])),
     ?_assertEqual(1, safe_length([null, 99, null])),
     ?_assertEqual(1, safe_length([{100, null}, {110, 99}, {120, null}]))
    ].

percentile_test_() ->
    [?_assertEqual(null, percentile([], 50)),
     ?_assertEqual(null, percentile([null], 50)),
     ?_assertEqual(1, percentile([1], 50)),
     ?_assertEqual(1, percentile([1, null, null], 50)),
     ?_assertEqual(2, percentile([1, null, 2], 50)),
     ?_assertEqual(2, percentile([2, null, 1], 50)),
     ?_assertEqual(1.5, percentile([2, null, 1], 50, true)),
     ?_assertEqual(2, percentile([2, null, 1, 3], 50)),
     ?_assertEqual(2.0, percentile([2, null, 1, 3], 50, true)),
     ?_assertEqual(1, percentile([2, 1, 3], 10)),
     ?_assertEqual(1.0, percentile([2, 1, 3], 10, true)),
     ?_assertEqual(3, percentile([2, 1, 3], 99)),
     ?_assertEqual(3, percentile([2, 1, 3], 99, true))
    ].

-endif.
