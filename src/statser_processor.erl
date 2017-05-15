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

% limit
evaluate_call(<<"limit">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    lists:sublist(Series, N);

% nPercentile
evaluate_call(<<"nPercentile">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      Perc = percentile(Values0, N),
                      Values = process_series_values(Values0, fun(_) -> Perc end),
                      S#series{values=Values}
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


square_sum([]) -> 0;
square_sum(Values) ->
    Avg = safe_average(Values),
    square_sum(Values, Avg, 0, 0).

square_sum([], Avg, Sum, Len) ->
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
    {Lst, _Idx} = lists:foldr(fun(V, {Acc, Idx}) -> {[{Idx, V} | Acc], Idx + 10} end, {[], 100}, Series),
    Lst.

pseudo_series(Values) ->
    Target = <<"target">>,
    [#series{target=Target,values=pseudo_values(Values)}].

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
    Series = pseudo_series([5.0, 7.0]),
    [?_assertEqual(Series, evaluate_call(<<"averageAbove">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual([], evaluate_call(<<"averageAbove">>, [Series, 6.5], 0, 0, 0))
    ].

average_below_test_() ->
    Series = pseudo_series([5.0, 7.0]),
    [?_assertEqual([], evaluate_call(<<"averageBelow">>, [Series, 5.5], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageBelow">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageBelow">>, [Series, 10], 0, 0, 0))
    ].

npercentile_test_() ->
    Series = pseudo_series([5.0, 8.0, 7.0]),
    [?_assertEqual(pseudo_series([7.0, 7.0, 7.0]), evaluate_call(<<"nPercentile">>, [Series, 50], 0, 0, 0)),
     ?_assertEqual(pseudo_series([5.0, 5.0, 5.0]), evaluate_call(<<"nPercentile">>, [Series, 10], 0, 0, 0)),
     ?_assertEqual(pseudo_series([8.0, 8.0, 8.0]), evaluate_call(<<"nPercentile">>, [Series, 99], 0, 0, 0))
    ].

average_outside_percentile_test_() ->
    S1 = pseudo_series([3.0, 5.0, 4.0]), % avg 4.0
    S2 = pseudo_series([3.0, 9.0, 6.0]), % avg 6.0
    S3 = pseudo_series([3.0, 12.0, 9.0]), % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(S1 ++ S3, evaluate_call(<<"averageOutsidePercentile">>, [Series, 80], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageOutsidePercentile">>, [Series, 50], 0, 0, 0))
    ].

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
