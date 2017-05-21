-module(statser_processor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([fetch_data/4,
         evaluate_call/5]).

-define(TIMEOUT, 2000).


fetch_data(Paths, From, Until, Now) ->
    % TODO: asynchronous fetching would be really nice
    lists:map(fun (Path) ->
                      % get metrics handler
                      case ets:lookup(metrics, Paths) of
                          [] ->
                              % there is no metrics handler already meaning
                              % there is nothing cached to be merged
                              % -> just read from fs directly instead
                              File = statser_metric_handler:get_whisper_file(Path),
                              Result = statser_whisper:fetch(File, From, Until, Now),
                              Result#series{target=Path};
                          [{_Path, Pid}] ->
                              Result = gen_server:call(Pid, {fetch, From, Until, Now}, ?TIMEOUT),
                              Result#series{target=Path}
                      end
              end, Paths).


% absolute
evaluate_call(<<"absolute">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S#series{values=process_series_values(S#series.values, fun erlang:abs/1)}
              end, Series);

% alias
evaluate_call(<<"alias">>, [Series, Alias], _From, _Until, _Now) when is_binary(Alias) ->
    lists:map(fun(S) -> S#series{target=Alias} end, Series);

% aliasByMetric
evaluate_call(<<"aliasByMetric">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) -> alias_target(S, [-1]) end, Series);

% aliasByNode
evaluate_call(<<"aliasByNode">>, [Series | Aliases], _From, _Until, _Now) ->
    lists:map(fun(S) -> alias_target(S, Aliases) end, Series);

% averageAbove
evaluate_call(<<"averageAbove">>, [Series, Avg], _From, _Until, _Now) ->
    lists:filter(fun(#series{values=Values}) -> safe_average(Values) >= Avg end, Series);

% averageBelow
evaluate_call(<<"averageBelow">>, [Series, Avg], _From, _Until, _Now) ->
    lists:filter(fun(#series{values=Values}) -> safe_average(Values) =< Avg end, Series);

% averageOutsidePercentile
evaluate_call(<<"averageOutsidePercentile">>, [Series, N0], _From, _Until, _Now) when is_number(N0) ->
    N = upper_half(N0),
    Avgs = lists:map(fun(#series{values=Values}) -> safe_average(Values) end, Series),
    LowPerc = percentile(Avgs, 100 - N),
    HighPerc = percentile(Avgs, N),
    lists:filter(fun(#series{values=Values}) ->
                         Avg = safe_average(Values),
                         Avg =< LowPerc orelse Avg >= HighPerc
                 end, Series);

% derivative
evaluate_call(<<"derivative">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) -> S#series{values=derivative(S#series.values)} end, Series);

% integral
evaluate_call(<<"integral">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      {Values, _} = lists:foldl(fun({_, null} = X, {Vs, Sum}) -> {[X | Vs], Sum};
                                                   ({TS, Val}, {Vs, Sum0}) ->
                                                        Sum = Sum0 + Val,
                                                        {[{TS, Sum} | Vs], Sum}
                                                end, {[], 0}, Values0),
                      ReversedValues = lists:reverse(Values),
                      S#series{values=ReversedValues}
              end, Series);

% invert
evaluate_call(<<"invert">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S#series{values=process_series_values(S#series.values, fun safe_invert/1)}
              end, Series);

% limit
evaluate_call(<<"limit">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    lists:sublist(Series, N);

% mostDeviant
evaluate_call(<<"mostDeviant">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    NumSeries = length(Series),
    case NumSeries =< N of
        true ->
            % no reason to calculate deviant if the requested count is less than
            % the total number of given series anyways
            Series;
        false ->
            SigmaSeries = lists:map(fun(S) ->
                                            SquareSum = square_sum(S#series.values),
                                            {SquareSum, S}
                                    end, Series),
            Sorted = lists:sort(fun({SigmaA, _}, {SigmaB, _}) -> SigmaA > SigmaB end, SigmaSeries),
            lists:map(fun({_Sigma, S}) -> S end, lists:sublist(Sorted, N))
    end;

% nPercentile
evaluate_call(<<"nPercentile">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      Perc = percentile(Values0, N),
                      Values = process_series_values(Values0, fun(_) -> Perc end),
                      S#series{values=Values}
              end, Series);

% offset
evaluate_call(<<"offset">>, [Series, Offset], _From, _Until, _Now) when is_number(Offset) ->
    lists:map(fun(S) ->
                      S#series{values=process_series_values(S#series.values, fun(X) -> X + Offset end)}
              end, Series);

% offsetToZero
evaluate_call(<<"offsetToZero">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Values = S#series.values,
                      Minimum = safe_minimum(Values),
                      S#series{values=process_series_values(Values, fun(X) -> X - Minimum end)}
              end, Series);

% removeAboveValue
evaluate_call(<<"removeAboveValue">>, [Series, Val], _From, _Until, _Now) when is_number(Val) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      Values = process_series_values(Values0, fun(X) when X > Val -> null;
                                                                 (X) -> X
                                                              end),
                      S#series{values=Values}
              end, Series);

% removeBelowValue
evaluate_call(<<"removeBelowValue">>, [Series, Val], _From, _Until, _Now) when is_number(Val) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      Values = process_series_values(Values0, fun(X) when X < Val -> null;
                                                                 (X) -> X
                                                              end),
                      S#series{values=Values}
              end, Series);

% squareRoot
evaluate_call(<<"squareRoot">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S#series{values=process_series_values(S#series.values, fun safe_square_root/1)}
              end, Series);

evaluate_call(Unknown, _Args, _From, _Until, _Now) ->
    lager:error("unknown function call ~p or invalid arguments", [Unknown]),
    error.


upper_half(N) when N < 50 -> 100 - N;
upper_half(N) -> N.


process_series_values(Series, Func) ->
    lists:map(fun ({_, null} = T) -> T;
                  ({TS, Value}) -> {TS, Func(Value)}
              end, Series).


alias_target(S, []) -> S;
alias_target(S, Aliases) ->
    Parts = binary:split(S#series.target, <<".">>, [global, trim_all]),
    NumParts = length(Parts),
    Target = to_target(lists:map(fun(Idx) -> get_part(Parts, NumParts, Idx) end, Aliases)),
    S#series{target=Target}.


get_part(Parts, _Length, Idx) when Idx >= 0 ->
    lists:nth(Idx + 1, Parts);
get_part(Parts, Length, Idx) ->
    lists:nth(Length + Idx + 1, Parts).


to_target(Parts) ->
    F = fun(A, <<>>) -> <<A/binary>>;
           (A, B) -> <<A/binary, ".", B/binary>>
        end,
    lists:foldr(F, <<>>, Parts).


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


derivative(Values) ->
    derivative(Values, null, []).

derivative([], _Prev, Acc) ->
    lists:reverse(Acc);
derivative([{TS, Value} | Vs], null, Acc) ->
    derivative(Vs, Value, [{TS, null} | Acc]);
derivative([{TS, null} | Vs], _Prev, Acc) ->
    derivative(Vs, null, [{TS, null} | Acc]);
derivative([{TS, Value} | Vs], Prev, Acc) ->
    derivative(Vs, Value, [{TS, Value - Prev} | Acc]).


% greatest common divisor
gcd(A, 0) -> A;
gcd(A, B) -> gcd(B, A rem B).


% least common multiple
lcm(A, A) -> A;
lcm(A, B) when A < B ->
    B div gcd(B, A) * A;
lcm(A, B)  ->
    A div gcd(A, B) * B.


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


floor(X) when X < 0 ->
    Truncated = trunc(X),
    case X - Truncated == 0 of
       true -> Truncated;
       false -> Truncated - 1
    end;
floor(X) ->
    trunc(X).


ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    Truncated = trunc(X),
    case X - Truncated == 0 of
       true -> Truncated;
       false -> Truncated + 1
    end.


consolidate(Series, ValuesPP) when ValuesPP =< 1 -> Series;
consolidate(Series, ValuesPP) ->
    Aggregate = Series#series.aggregation,
    Values = consolidate_values(Series#series.values, ValuesPP, Aggregate),
    Step = Series#series.step * ValuesPP,
    Series#series{values=Values,step=Step}.


consolidate_values([], _ValuesPP, _Aggregate) -> [];
consolidate_values([{TS0, Val0} | Tl], ValuesPP, Aggregate) ->
    {Vs, Cs, TS, _} = lists:foldl(fun({TS, null}, {Result, ConsVs, Ts, Cnt}) when Cnt == ValuesPP ->
                                          Agg = statser_whisper:aggregate(Aggregate, ConsVs, length(ConsVs), Cnt),
                                          {[{Ts, Agg} | Result], [], TS, 1};
                                     ({TS, Val}, {Result, ConsVs, Ts, Cnt}) when Cnt == ValuesPP ->
                                          Agg = statser_whisper:aggregate(Aggregate, ConsVs, length(ConsVs), Cnt),
                                          {[{Ts, Agg} | Result], [Val], TS, 1};
                                     ({_TS, null}, {Result, ConsVs, TS, Cnt}) ->
                                          {Result, ConsVs, TS, Cnt+1};
                                     ({_TS, Val}, {Result, ConsVs, TS, Cnt}) ->
                                          {Result, [Val | ConsVs], TS, Cnt+1}
                                  end, {[], [Val0], TS0, 1}, Tl),
    case Cs of
        [] -> lists:reverse(Vs);
        _ ->
            LastValue = statser_whisper:aggregate(Aggregate, Cs, length(Cs), ValuesPP),
            lists:reverse([{TS, LastValue} | Vs])
    end.


normalize(SeriesLst) ->
    Series = lists:flatten(SeriesLst),
    {_Start, _End, Step} = normalize_stats(Series),
    % TODO: properly handle start/stop
    % TODO: this is sufficient for now as most of the time start/end are the same over all series
    lists:map(fun(S) -> consolidate(S, Step div S#series.step) end, Series).


normalize_stats([Hd | Tl]) ->
    Acc = {Hd#series.start, Hd#series.until, Hd#series.step},
    {S, End0, Step} = lists:foldr(fun(S, {Start0, End0, Step0}) ->
                                          Start = min(Start0, S#series.start),
                                          End = max(End0, S#series.until),
                                          Step = lcm(Step0, S#series.step),
                                          {Start, End, Step}
                                  end, Acc, Tl),
    End = (End0 - S) rem Step,
    {S, End, Step}.


zip_lists([], _WithFunc) -> [];
zip_lists(Lists, WithFunc) ->
    zip_lists(Lists, WithFunc, []).

zip_lists(Lists, WithFunc, Acc) ->
    case zip_heads(Lists, WithFunc) of
        undefined -> lists:reverse(Acc);
        {Hd, Tails} -> zip_lists(Tails, WithFunc, [Hd | Acc])
    end.


zip_heads(Lists, Func) ->
    Lsts = lists:foldl(fun([], _Acc) -> undefined;
                          (_, undefined) -> undefined;
                          ([Hd | Tl], {Heads, Tails}) ->
                               % appending (`++`) should be fine in here because the
                               % number of lists will be rather small compared to
                               % the number of values of each list
                               {Heads ++ [Hd], [Tl | Tails]}
                       end, {[], []}, Lists),
    case Lsts of
        undefined -> undefined;
        {Heads, Tails} ->
            {Func(Heads), Tails}
    end.


safe_minimum([]) -> null;
safe_minimum(Values) ->
    safe_minimum(Values, null).

safe_minimum([], Min) -> Min;
safe_minimum([{_TS, null} | Vs], Min) ->
    safe_minimum(Vs, Min);
safe_minimum([{_TS, Value} | Vs], Min) ->
    safe_minimum(Vs, safe_minimum0(Min, Value));
safe_minimum([null | Vs], Min) ->
    safe_minimum(Vs, Min);
safe_minimum([Value | Vs], Min) ->
    safe_minimum(Vs, safe_minimum0(Min, Value)).


safe_minimum0(null, Value) -> Value;
safe_minimum0(Value, null) -> Value;
safe_minimum0(A, B) -> min(A, B).


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


square_sum([]) -> 0;
square_sum(Values) ->
    Avg = safe_average(Values),
    square_sum(Values, Avg, 0, 0).

square_sum([], _Avg, Sum, Len) ->
    Sum / Len;
square_sum([{_TS, null} | Vs], Avg, Sum, Len) ->
    square_sum(Vs, Avg, Sum, Len);
square_sum([{_TS, Val} | Vs], Avg, Sum, Len) ->
    Square = math:pow(Val - Avg, 2),
    square_sum(Vs, Avg, Sum + Square, Len + 1);
square_sum([null | Vs], Avg, Sum, Len) ->
    square_sum(Vs, Avg, Sum, Len);
square_sum([Val | Vs], Avg, Sum, Len) ->
    Square = math:pow(Val - Avg, 2),
    square_sum(Vs, Avg, Sum + Square, Len + 1).


percentile(Values, N) ->
    percentile(Values, N, false).

percentile(Values, N, Interpolate) ->
    {Sorted, Len} = sort_non_null(Values),
    FractionalRank = (N / 100.0) * (Len + 1),
    Rank0 = floor(FractionalRank),
    RankFraction = FractionalRank - Rank0,

    Rank =
    if Interpolate == true -> Rank0;
       true -> Rank0 + ceiling(RankFraction)
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

alias_target_test_() ->

    [?_assertEqual(pseudo_target(<<"foo">>), alias_target(pseudo_target(<<"foo">>), [])),
     ?_assertEqual(pseudo_target(<<"foo.bar">>), alias_target(pseudo_target(<<"foo.bar">>), [])),
     ?_assertEqual(pseudo_target(<<"test">>), alias_target(pseudo_target(<<"foo.bar.test">>), [2])),
     ?_assertEqual(pseudo_target(<<"test">>), alias_target(pseudo_target(<<"foo.bar.test">>), [-1])),
     ?_assertEqual(pseudo_target(<<"foo">>), alias_target(pseudo_target(<<"foo.bar.test">>), [0]))
    ].

pseudo_target(Target) ->
    #series{target=Target}.

pseudo_values(Series) ->
    pseudo_values(Series, 10).

pseudo_values(Series, Step) ->
    {Lst, _Idx} = lists:foldl(fun(V, {Acc, Idx}) -> {[{Idx, V} | Acc], Idx + Step} end, {[], 100}, Series),
    lists:reverse(Lst).

pseudo_series(Values) ->
    pseudo_series(Values, 10).

pseudo_series(Values, Step) ->
    pseudo_series(Values, Step, average).

pseudo_series(Values, Step, Aggregation) ->
    Target = <<"target">>,
    Start = 100,
    End = length(Values) * Step + Start,
    PValues = pseudo_values(Values, Step),
    #series{target=Target,values=PValues,start=Start,until=End,step=Step,aggregation=Aggregation}.

derivative_test_() ->
    [?_assertEqual([], derivative([])),
     ?_assertEqual(pseudo_values([null,null,1,1,1,1,null,null,1,1]),
                   derivative(pseudo_values([null,1,2,3,4,5,null,6,7,8]))),
     ?_assertEqual(pseudo_values([null,1,2,2,2]),
                   derivative(pseudo_values([1,2,4,6,8])))
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

average_above_test_() ->
    Series = [pseudo_series([5.0, 7.0])],
    [?_assertEqual(Series, evaluate_call(<<"averageAbove">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual([], evaluate_call(<<"averageAbove">>, [Series, 6.5], 0, 0, 0))
    ].

average_below_test_() ->
    Series = [pseudo_series([5.0, 7.0])],
    [?_assertEqual([], evaluate_call(<<"averageBelow">>, [Series, 5.5], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageBelow">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageBelow">>, [Series, 10], 0, 0, 0))
    ].

npercentile_test_() ->
    Series = [pseudo_series([5.0, 8.0, 7.0])],
    [?_assertEqual([pseudo_series([7.0, 7.0, 7.0])], evaluate_call(<<"nPercentile">>, [Series, 50], 0, 0, 0)),
     ?_assertEqual([pseudo_series([5.0, 5.0, 5.0])], evaluate_call(<<"nPercentile">>, [Series, 10], 0, 0, 0)),
     ?_assertEqual([pseudo_series([8.0, 8.0, 8.0])], evaluate_call(<<"nPercentile">>, [Series, 99], 0, 0, 0))
    ].

average_outside_percentile_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(S1 ++ S3, evaluate_call(<<"averageOutsidePercentile">>, [Series, 80], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageOutsidePercentile">>, [Series, 50], 0, 0, 0))
    ].

most_deviant_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(Series, evaluate_call(<<"mostDeviant">>, [Series, 3], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"mostDeviant">>, [Series, 90], 0, 0, 0)),
     ?_assertEqual(S3 ++ S2, evaluate_call(<<"mostDeviant">>, [Series, 2], 0, 0, 0)),
     ?_assertEqual(S3, evaluate_call(<<"mostDeviant">>, [Series, 1], 0, 0, 0))
    ].

integral_test_() ->
    [?_assertEqual([pseudo_series([1,2,3])], evaluate_call(<<"integral">>, [[pseudo_series([1,1,1])]], 0, 0, 0)),
     ?_assertEqual([pseudo_series([1,null,2,3])], evaluate_call(<<"integral">>, [[pseudo_series([1,null,1,1])]], 0, 0, 0)),
     ?_assertEqual([pseudo_series([])], evaluate_call(<<"integral">>, [[pseudo_series([])]], 0, 0, 0)),
     ?_assertEqual([pseudo_series([null,null])], evaluate_call(<<"integral">>, [[pseudo_series([null,null])]], 0, 0, 0))
    ].

offset_to_zero_test_() ->
    Series = [pseudo_series([102, 101, 104, 101, 100, 111])],
    Expected = [pseudo_series([2, 1, 4, 1, 0, 11])],
    [?_assertEqual(Expected, evaluate_call(<<"offsetToZero">>, [Series], 0, 0, 0))].

sort_non_null_test_() ->
    [?_assertEqual({[], 0}, sort_non_null([])),
     ?_assertEqual({[], 0}, sort_non_null([null, null])),
     ?_assertEqual({[1], 1}, sort_non_null([1])),
     ?_assertEqual({[1, 2], 2}, sort_non_null([1, null, 2])),
     ?_assertEqual({[1, 2], 2}, sort_non_null([2, null, 1]))
    ].

square_sum_test_() ->
    [?_assertEqual(0, square_sum([])),
     ?_assertEqual(2/3, square_sum([3, 4, 5])),
     ?_assertEqual(0.0, square_sum([3, 3, null, 3, null]))
    ].

minimum_test_() ->
    [?_assertEqual(0, safe_minimum([23, 0])),
     ?_assertEqual(0, safe_minimum([23, 0, 1, null])),
     ?_assertEqual(-1, safe_minimum([null, 0, -1, null])),
     ?_assertEqual(-1, safe_minimum(pseudo_values([null, 0, -1, null]))),
     ?_assertEqual(5, safe_minimum(pseudo_values([null, 101, 5, null, 32]))),
     ?_assertEqual(null, safe_minimum([]))
    ].

lcm_test_() ->
    [?_assertEqual(0, lcm(10, 0)),
     ?_assertEqual(0, lcm(0, 5)),
     ?_assertEqual(10, lcm(10, 5))
    ].

gcd_test_() ->
    [?_assertEqual(10, gcd(10, 0)),
     ?_assertEqual(5, gcd(0, 5)),
     ?_assertEqual(5, gcd(10, 5))
    ].

safe_diff_test_() ->
    [?_assertEqual(0, safe_diff([])),
     ?_assertEqual(1, safe_diff([1])),
     ?_assertEqual(0, safe_diff([1, 1])),
     ?_assertEqual(-5, safe_diff([1, 1, 2, 3])),
     ?_assertEqual(-5, safe_diff([null, 1, 1, 2, null, 3, null])),
     ?_assertEqual(0, safe_diff([null, null, null]))
    ].

consolidate_test_() ->
    Series = pseudo_series([1,2,1,2,1,2,1,5]),
    Expected = pseudo_series([1.5,1.5,1.5,3.0], 20),
    [?_assertEqual(Expected, consolidate(Series, 2)),
     ?_assertEqual(pseudo_series([1.5], 30), consolidate(pseudo_series([1, null, 2]), 3)),
     ?_assertEqual(pseudo_series([1.5, 1.0], 30), consolidate(pseudo_series([1, null, 2, null, null, 1]), 3)),
     ?_assertEqual(pseudo_series([], 20), consolidate(pseudo_series([]), 2)),
     ?_assertEqual(pseudo_series([2,2,2], 20, sum), consolidate(pseudo_series([1,1,1,1,1,1], 10, sum), 2))
    ].

normalize_test_() ->
    Series = [pseudo_series([1,1,1], 20), pseudo_series([2,2,2,2,2,2])],
    [?_assertEqual([pseudo_series([1,1,1], 20), pseudo_series([2.0,2.0,2.0], 20)], normalize([Series]))].

zip_lists_test_() ->
    [?_assertEqual([], zip_lists([], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3], [1,2,3]], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3,4], [1,2,3]], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3], [1,2,3,4]], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3], [1,2,3,4,5,6,7]], fun lists:sum/1)),
     ?_assertEqual([3, 6, 9], zip_lists([[1,2,3], [1,2,3], [1,2,3]], fun lists:sum/1))
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
