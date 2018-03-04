% Copyright 2017-2018 Gregor Uhlenheuer
%
% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
% You may obtain a copy of the License at
%
%     http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.

-module(statser_processor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

% base API
-export([consolidate/2,
         evaluate_call/5,
         fetch_data/4]).

-define(TIMEOUT, 2000).
-define(FETCHER_TIMEOUT, 2500).

-spec fetch_data([binary()], integer(), integer(), integer()) -> [#series{}].
fetch_data([], _From, _Until, _Now) -> [];

fetch_data([Path], From, Until, Now) ->
    % if only *one* path is requested in here, there is no reason
    % to spawn multiple parallel fetch processes
    fetch_inner(Path, From, Until, Now);

fetch_data(Paths, From, Until, Now) ->
    Parent = self(),
    % spawn fetchers who are reporting back to `Parent`
    Fetchers = [spawn_link(fun() -> Parent ! {self(), fetch_inner(Path, From, Until, Now)} end) ||
                Path <- Paths],
    % collect fetchers' results
    lists:flatmap(fun(_Fetcher) ->
                          receive {_Pid, SeriesData} -> SeriesData
                          after ?FETCHER_TIMEOUT -> []
                          end
                  end, Fetchers).


% if the arguments contain a valid pid we can directly call
% that metric_handler instead of requesting the ETS table
% this should usually be the case as soon as the metric_handler
% is registered with the statser_finder
fetch_inner({Path, {local, Pid}}, From, Until, Now) when is_pid(Pid) ->
    Result = gen_server:call(Pid, {fetch, From, Until, Now}, ?TIMEOUT),
    case Result of
        #series{} -> [Result#series{target=Path}];
        _Error -> []
    end;

% we do have a remote pid to call directly -> let's do that
fetch_inner({Path, {remote, Node, Pid}}, From, Until, Now) when is_pid(Pid) ->
    lager:debug("fetching from remote [~p] ~p: '~s'", [Node, Pid, Path]),

    try
        Result = gen_server:call(Pid, {fetch, From, Until, Now}, ?TIMEOUT),
        case Result of
            #series{} -> [Result#series{target=Path}];
            _Error -> []
        end
    catch
        exit:{{nodedown, Node}, _} ->
            lager:warning("remote fetch call failed - node ~p is down", [Node]),

            % 'nodedown' is a clear indication that the given node is not available
            % anymore - but we did try to reach it just now
            % that's why we publish a 'down' for that given node using the event bus
            statser_event:notify({down, Node}),

            [];
        Class:Reason ->
            lager:warning("remote fetch call failed with: ~p:~p", [Class, Reason]),
            []
    end;

% this is a remote metrics but without an associated handler pid
% so we just call the remote's dispatcher for now
fetch_inner({Path, {remote, Node, _Pid}}, From, Until, Now) ->
    lager:debug("fetching from remote [~p] via dispatcher: '~s'", [Node, Path]),

    try
        gen_server:call({statser_dispatcher, Node}, {fetch, Path, From, Until, Now}, ?TIMEOUT)
    catch
        exit:{{nodedown, Node}, _} ->
            lager:warning("remote fetch call failed - node ~p is down", [Node]),

            % 'nodedown' is a clear indication that the given node is not available
            % anymore - but we did try to reach it just now
            % that's why we publish a 'down' for that given node using the event bus
            statser_event:notify({down, Node}),

            [];
        Class:Reason ->
            lager:warning("remote fetch call failed with: ~p:~p", [Class, Reason]),
            []
    end;

% otherwise we will request the metric_handler's pid from the
% ETS table or even fetch the file directly from disk instead
fetch_inner({Path, _Handler}, From, Until, Now) ->
    statser_dispatcher:fetch(Path, From, Until, Now).


% absolute
evaluate_call(<<"absolute">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S0 = S#series{values=process_series_values(S#series.values, fun erlang:abs/1)},
                      with_function_name(S0, "absolute")
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

% aliasSub
evaluate_call(<<"aliasSub">>, [Series, Search, Replace], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Target = binary_to_list(S#series.target),
                      Replaced = re:replace(Target, Search, Replace, [{return, binary}]),
                      S#series{target=Replaced}
              end, Series);

% averageAbove
evaluate_call(<<"averageAbove">>, [Series, Avg], _From, _Until, _Now) ->
    filter_named(fun(#series{values=Values}) -> statser_calc:safe_average(Values) >= Avg end, Series, "averageAbove");

% averageBelow
evaluate_call(<<"averageBelow">>, [Series, Avg], _From, _Until, _Now) ->
    filter_named(fun(#series{values=Values}) -> statser_calc:safe_average(Values) =< Avg end, Series, "averageBelow");

% averageOutsidePercentile
evaluate_call(<<"averageOutsidePercentile">>, [Series, N0], _From, _Until, _Now) when is_number(N0) ->
    N = upper_half(N0),
    Avgs = lists:map(fun(#series{values=Values}) -> statser_calc:safe_average(Values) end, Series),
    LowPerc = statser_calc:percentile(Avgs, 100 - N),
    HighPerc = statser_calc:percentile(Avgs, N),
    filter_named(fun(#series{values=Values}) ->
                         Avg = statser_calc:safe_average(Values),
                         Avg =< LowPerc orelse Avg >= HighPerc
                 end, Series, "averageOutsidePercentile");

% alias for 'averageSeries'
evaluate_call(<<"avg">>, Series, From, Until, Now) ->
    evaluate_call(<<"averageSeries">>, Series, From, Until, Now);

% averageSeries
evaluate_call(<<"averageSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_average/1, "averageSeries");

% asPercent
%
% The 'original' graphite API is a real mess because the function may
% be called in three or four different variants.
% We will support only a part of what is (currently) supported by
% graphite already:
%
%  * 'total' is not specified at all (use the series' sum instead)
%  * 'total' is specified as a constant number
%  * 'total' is specified as one or more series
%
evaluate_call(<<"asPercent">>, [Series, Total], _From, _Until, _Now) when is_number(Total) ->
    ToPercent = Total / 100.0,
    lists:map(fun(S) ->
                      Vs = lists:map(fun(V) -> statser_calc:safe_div(V, ToPercent) end, S#series.values),
                      with_function_name(S#series{values=Vs}, "asPercent")
              end, Series);

evaluate_call(<<"asPercent">>=Call, [Series], From, Until, Now) ->
    % we pass in ourselves to the 'overload' that takes the 'total-series'
    % so we can calculate the total based on it
    evaluate_call(Call, [Series, Series], From, Until, Now);

evaluate_call(<<"asPercent">>, [Series, TotalSeries], _From, _Until, _Now) ->
    % create 'sum-series' first
    case normalize(TotalSeries) of
        {[], _, _, _} -> [];
        {SummedS0, _Start, _End, _Step} ->
            [SummedS] = zip_series(SummedS0, fun statser_calc:safe_sum/1, "__summed__"),

            % usually we zip into *one* series but this time we have to map over each series
            % and zip these with the 'sum-series' from above
            lists:flatmap(fun(S) ->
                                  {Norm, _Start, _End, _Step} = normalize([S, SummedS]),
                                  Percent = fun([V, Total]) ->
                                                    statser_calc:safe_div(V, Total / 100.0)
                                            end,
                                  zip_series(Norm, Percent, "asPercent")
                          end, Series)
    end;

% changed
evaluate_call(<<"changed">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S0 = S#series{values=changed(S#series.values)},
                      with_function_name(S0, "changed")
              end, Series);

% currentAbove
evaluate_call(<<"currentAbove">>, [Series, Threshold], _From, _Until, _Now) when is_number(Threshold) ->
    filter_named(fun(#series{values=Values}) ->
                         case safe_last(Values) of
                             null -> false;
                             {_TS, null} -> false;
                             {_TS, Val} ->
                                 Val > Threshold
                         end
                 end, Series, "currentAbove");

% currentBelow
evaluate_call(<<"currentBelow">>, [Series, Threshold], _From, _Until, _Now) when is_number(Threshold) ->
    filter_named(fun(#series{values=Values}) ->
                         case safe_last(Values) of
                             null -> false;
                             {_TS, null} -> false;
                             {_TS, Val} ->
                                 Val < Threshold
                         end
                 end, Series, "currentBelow");

% derivative
evaluate_call(<<"derivative">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S0 = S#series{values=derivative(S#series.values)},
                      with_function_name(S0, "derivative")
              end, Series);

% divideSeries
evaluate_call(<<"divideSeries">>, [Series1, Series2], _From, _Until, _Now) ->
    % TODO: check series' lengths
    Combine = fun(S1, S2) ->
                      Dividend = S1#series.values,
                      Divisor = S2#series.values,
                      Res = lists:zipwith(fun statser_calc:safe_div/2, Dividend, Divisor),
                      Series = S1#series{values=Res},
                      with_function_name(Series, "divideSeries")
              end,
    lists:zipwith(Combine, Series1, Series2);

% diffSeries
evaluate_call(<<"diffSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_diff/1, "diffSeries");

% exclude
evaluate_call(<<"exclude">>, [Series, Pattern], _From, _Until, _Now) ->
    case re:compile(Pattern) of
        {ok, Regex} ->
            lists:filter(fun(#series{target=Target}) ->
                                 case re:run(Target, Regex) of
                                     {match, _Mtch} -> false;
                                     _Otherwise -> true
                                 end
                         end, Series);
        {error, _Error} ->
            Series
    end;

% grep
evaluate_call(<<"grep">>, [Series, Pattern], _From, _Until, _Now) ->
    case re:compile(Pattern) of
        {ok, Regex} ->
            lists:filter(fun(#series{target=Target}) ->
                                 case re:run(Target, Regex) of
                                     {match, _Mtch} -> true;
                                     _Otherwise -> false
                                 end
                         end, Series);
        {error, _Error} ->
            []
    end;

% highestAverage
evaluate_call(<<"highestAverage">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    select_top_n(Series, N, "highestAverage", fun statser_calc:safe_average/1);

% highestCurrent
evaluate_call(<<"highestCurrent">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    Func = fun(Values) ->
                   case safe_last(Values) of
                       {_TS, null} -> 0;
                       {_TS, Val} -> Val
                   end
           end,
    select_top_n(Series, N, "highestCurrent", Func);

% highestMax
evaluate_call(<<"highestMax">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    select_top_n(Series, N, "highestMax", fun statser_calc:safe_max/1);

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
                      S0 = S#series{values=ReversedValues},
                      with_function_name(S0, "integral")
              end, Series);

% integralByInterval
evaluate_call(<<"integralByInterval">>, [Series, Interval], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      ISecs = if
                                  is_binary(Interval) -> statser_util:parse_unit(Interval);
                                  true -> Interval * S#series.step
                              end,
                      Vs = integral_by_interval(S#series.values, ISecs),
                      with_function_name(S#series{values=Vs}, "integralByInterval")
              end, Series);

% invert
evaluate_call(<<"invert">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S0 = S#series{values=process_series_values(S#series.values, fun statser_calc:safe_invert/1)},
                      with_function_name(S0, "invert")
              end, Series);

% isNonNull
evaluate_call(<<"isNonNull">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Vs = lists:map(fun({TS, null}) -> {TS, 0};
                                        ({TS, _Val}) -> {TS, 1}
                                     end, S#series.values),
                      with_function_name(S#series{values=Vs}, "isNonNull")
              end, Series);

% keepLastValue
evaluate_call(<<"keepLastValue">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Vs = keep_last_value(S#series.values),
                      S#series{values=Vs}
              end, Series);

% limit
evaluate_call(<<"limit">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    lists:map(fun(S) -> with_function_name(S, "limit") end, lists:sublist(Series, N));

% lowestAverage
evaluate_call(<<"lowestAverage">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    select_low_n(Series, N, "lowestAverage", fun statser_calc:safe_average/1);

% lowestCurrent
evaluate_call(<<"lowestCurrent">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    Func = fun(Values) ->
                   {_TS, Val} = safe_last(Values),
                   Val
           end,
    select_low_n(Series, N, "lowestCurrent", Func);

% maximumAbove
evaluate_call(<<"maximumAbove">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    filter_named(fun(#series{values=Values}) ->
                         case statser_calc:safe_max(Values) of
                             null -> false;
                             X -> X > N
                         end
                 end,
                 Series, "maximumAbove");

% maximumBelow
evaluate_call(<<"maximumBelow">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    filter_named(fun(#series{values=Values}) -> statser_calc:safe_max(Values) < N end,
                 Series, "maximumBelow");

% maxSeries
evaluate_call(<<"maxSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_max/1, "maxSeries");

% minimumAbove
evaluate_call(<<"minimumAbove">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    filter_named(fun(#series{values=Values}) -> statser_calc:safe_min(Values) > N end,
                 Series, "minimumAbove");

% minimumBelow
evaluate_call(<<"minimumBelow">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    filter_named(fun(#series{values=Values}) -> statser_calc:safe_min(Values) < N end,
                 Series, "minimumBelow");

% minSeries
evaluate_call(<<"minSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_min/1, "minSeries");

% mostDeviant
evaluate_call(<<"mostDeviant">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    select_top_n(Series, N, "mostDeviant", fun square_sum/1);

% movingAverage
evaluate_call(<<"movingAverage">>, [Series, Window], From, Until, Now) ->
    lists:map(fun(S0) ->
                      Step = S0#series.step,
                      Points = if
                                   is_binary(Window) -> statser_util:parse_unit(Window) div Step;
                                   true -> Window
                               end,
                      % fetch additional past data
                      [S] = fetch_data([S0#series.target], From-(Points * Step), Until, Now),
                      Values = moving_average(S#series.values, Points),
                      with_function_name(S0#series{values=Values}, "movingAverage")
              end, Series);

% multiplySeries
evaluate_call(<<"multiplySeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun safe_multiply/1, "multiplySeries");

% nPercentile
evaluate_call(<<"nPercentile">>, [Series, N], _From, _Until, _Now) when is_number(N) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      Perc = statser_calc:percentile(Values0, N),
                      Values = process_series_values(Values0, fun(_) -> Perc end),
                      with_function_name(S#series{values=Values}, "nPercentile")
              end, Series);

% nonNegativeDerivative
evaluate_call(<<"nonNegativeDerivative">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Values = non_negative_derivative(S),
                      S0 = S#series{values=Values},
                      with_function_name(S0, "nonNegativeDerivative")
              end, Series);

% offset
evaluate_call(<<"offset">>, [Series, Offset], _From, _Until, _Now) when is_number(Offset) ->
    lists:map(fun(S) ->
                      S0 = S#series{values=process_series_values(S#series.values, fun(X) -> X + Offset end)},
                      with_function_name(S0, "offset")
              end, Series);

% perSecond
evaluate_call(<<"perSecond">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Values = per_second(S),
                      S0 = S#series{values=Values},
                      with_function_name(S0, "perSecond")
              end, Series);

% offsetToZero
evaluate_call(<<"offsetToZero">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      Values = S#series.values,
                      Minimum = safe_minimum(Values),
                      S0 = S#series{values=process_series_values(Values, fun(X) -> X - Minimum end)},
                      with_function_name(S0, "offsetToZero")
              end, Series);

% pow
evaluate_call(<<"pow">>, [Series, Factor], _From, _Until, _Now) when is_number(Factor) ->
    lists:map(fun(S) ->
                      Values = process_series_values(S#series.values,
                                                     fun(X) -> statser_calc:safe_pow([X, Factor]) end),
                      with_function_name(S#series{values=Values}, "pow")
              end, Series);

% powSeries
evaluate_call(<<"powSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_pow/1, "powSeries");

% randomWalk
evaluate_call(<<"randomWalk">>, [Target], From, Until, Now) ->
    evaluate_call(<<"randomWalk">>, [Target, 60], From, Until, Now);

% randomWalk
evaluate_call(<<"randomWalk">>, [Target, Step], From, Until, _Now) when is_number(Step) ->
    Length = (Until - From) div Step,
    Vs = lists:map(fun(Idx) -> {From + Step * Idx, rand:uniform()} end,
                   lists:seq(0, Length)),
    [#series{values=Vs, target=Target, start=From, until=Until, step=Step, aggregation=average}];

% rangeOfSeries
evaluate_call(<<"rangeOfSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_range/1, "rangeOfSeries");

% removeAboveValue
evaluate_call(<<"removeAboveValue">>, [Series, Val], _From, _Until, _Now) when is_number(Val) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      Values = process_series_values(Values0, fun(X) when X > Val -> null;
                                                                 (X) -> X
                                                              end),
                      S0 = S#series{values=Values},
                      with_function_name(S0, "removeAboveValue")
              end, Series);

% removeBelowValue
evaluate_call(<<"removeBelowValue">>, [Series, Val], _From, _Until, _Now) when is_number(Val) ->
    lists:map(fun(S) ->
                      Values0 = S#series.values,
                      Values = process_series_values(Values0, fun(X) when X < Val -> null;
                                                                 (X) -> X
                                                              end),
                      S0 = S#series{values=Values},
                      with_function_name(S0, "removeBelowValue")
              end, Series);

% scale
evaluate_call(<<"scale">>, [Series, Factor], _From, _Until, _Now) when is_number(Factor) ->
    lists:map(fun(S) ->
                      Scale = fun(V) -> V * Factor end,
                      S0 = S#series{values=process_series_values(S#series.values, Scale)},
                      with_function_name(S0, "scale")
              end, Series);

% scaleToSeconds
evaluate_call(<<"scaleToSeconds">>, [Series, Seconds], _From, _Until, _Now) when is_number(Seconds) andalso Seconds > 0 ->
    lists:map(fun(S) ->
                      Factor = Seconds / S#series.step,
                      Scale = fun(V) -> V * Factor end,
                      S0 = S#series{values=process_series_values(S#series.values, Scale)},
                      with_function_name(S0, "scaleToSeconds")
              end, Series);

% squareRoot
evaluate_call(<<"squareRoot">>, [Series], _From, _Until, _Now) ->
    lists:map(fun(S) ->
                      S0 = S#series{values=process_series_values(S#series.values,
                                                                 fun statser_calc:safe_square_root/1)},
                      with_function_name(S0, "squareRoot")
              end, Series);

% stddevSeries
evaluate_call(<<"stddevSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_stddev/1, "stddevSeries");

% sumSeries
evaluate_call(<<"sumSeries">>, Series, _From, _Until, _Now) ->
    {Norm, _Start, _End, _Step} = normalize(Series),
    zip_series(Norm, fun statser_calc:safe_sum/1, "sumSeries");

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
    Parts = statser_util:split_metric(S#series.target),
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


integral_by_interval([], _IntervalSeconds) -> [];
integral_by_interval([{TS, _} | _] = Vs, IntervalSeconds) ->
    integral_by_interval(Vs, TS, 0, [], IntervalSeconds).

integral_by_interval([], _LastInterval, _Sum, Result, _MaxInterval) ->
    lists:reverse(Result);
integral_by_interval([{TS, Val} | Vs], LastInterval, Sum, Result, MaxInterval) ->
    TimeSpan = TS - LastInterval,
    Value = case Val of
                null -> 0;
                _ -> Val
            end,
    if TimeSpan >= MaxInterval ->
           integral_by_interval(Vs, TS, Value, [{TS, Value} | Result], MaxInterval);
       true ->
           integral_by_interval(Vs, LastInterval, Sum + Value, [{TS, Sum + Value} | Result], MaxInterval)
    end.


% greatest common divisor
gcd(A, 0) -> A;
gcd(A, B) -> gcd(B, A rem B).


% least common multiple
lcm(A, A) -> A;
lcm(A, B) when A < B ->
    B div gcd(B, A) * A;
lcm(A, B)  ->
    A div gcd(A, B) * B.


select_top_n(Series, N, Name, Func) ->
    NumSeries = length(Series),
    case NumSeries =< N of
        true ->
            % no reason to select series if the total number is less than `N` anyways
            lists:map(fun(S) -> with_function_name(S, Name) end, Series);
        false ->
            XSeries = lists:map(fun(S) ->
                                        XVal = Func(S#series.values),
                                        {XVal, S}
                                end, Series),
            Sorted = lists:sort(fun({X, _}, {Y, _}) -> X > Y end, XSeries),
            lists:map(fun({_Avg, S}) -> with_function_name(S, Name) end,
                      lists:sublist(Sorted, N))
    end.


select_low_n(Series, N, Name, Func) ->
    NumSeries = length(Series),
    case NumSeries =< N of
        true ->
            % no reason to select series if the total number is less than `N` anyways
            lists:map(fun(S) -> with_function_name(S, Name) end, Series);
        false ->
            XSeries = lists:map(fun(S) ->
                                        XVal = Func(S#series.values),
                                        {XVal, S}
                                end, Series),
            Sorted = lists:sort(fun({X, _}, {Y, _}) -> X =< Y end, XSeries),
            lists:map(fun({_Avg, S}) -> with_function_name(S, Name) end,
                      lists:sublist(Sorted, N))
    end.


-spec consolidate(#series{}, integer()) -> #series{}.
consolidate(Series, ValuesPP) when ValuesPP =< 1 -> Series;
consolidate(Series, ValuesPP) ->
    Aggregate = Series#series.aggregation,
    Values = consolidate_values(Series#series.values, ValuesPP, Aggregate),
    Step = Series#series.step * ValuesPP,
    Series#series{values=Values,step=Step}.


consolidate_values([], _ValuesPP, _Aggregate) -> [];
consolidate_values([{TS0, Val0} | Tl], ValuesPP, Aggregate) ->
    Init = case Val0 of
               null -> {[], [], TS0, 1};
               _ -> {[], [Val0], TS0, 1}
           end,
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
                                  end, Init, Tl),
    case Cs of
        [] -> lists:reverse(Vs);
        _ ->
            LastValue = statser_whisper:aggregate(Aggregate, Cs, length(Cs), ValuesPP),
            lists:reverse([{TS, LastValue} | Vs])
    end.


normalize(SeriesLst) ->
    case lists:flatten(SeriesLst) of
        [] ->
            % TODO: not quite sure what to return in this case but I guess this way
            % is at least better than throwing an error...
            {[], null, null, null};
        Series ->
            {Start, End, Step} = normalize_stats(Series),
            % TODO: properly handle start/stop
            % TODO: this is sufficient for now as most of the time start/end are the same over all series
            Ss = lists:map(fun(S) -> consolidate(S, Step div S#series.step) end, Series),
            {Ss, Start, End, Step}
    end.


normalize_stats([Hd | Tl]) ->
    Acc = {Hd#series.start, Hd#series.until, Hd#series.step},
    {S, End0, Step} = lists:foldr(fun(S, {Start0, End0, Step0}) ->
                                          Start = min(Start0, S#series.start),
                                          End = max(End0, S#series.until),
                                          Step = lcm(Step0, S#series.step),
                                          {Start, End, Step}
                                  end, Acc, Tl),
    End = End0 + ((End0 - S) rem Step),
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
    % we are using `foldr` instead of `foldl` so we can process
    % the values in the same order as the series
    Lsts = lists:foldr(fun([], _Acc) -> undefined;
                          (_, undefined) -> undefined;
                          ([{TS, Val} | Tl], {Heads, Tails, _}) ->
                               {[Val | Heads], [Tl | Tails], TS};
                          ([Val | Tl], {Heads, Tails, TS}) ->
                               {[Val | Heads], [Tl | Tails], TS}
                       end, {[], [], null}, Lists),
    case Lsts of
        undefined -> undefined;
        {Heads, Tails, null} ->
            {Func(Heads), Tails};
        {Heads, Tails, TS} ->
            {{TS, Func(Heads)}, Tails}
    end.


zip_series([], _Func, _Name) -> [];
zip_series([Hd | _] = Series, Func, Name) ->
    Values = lists:map(fun(#series{values=Vs}) -> Vs end, Series),
    % we use the first series as a template
    Zipped = Hd#series{values=zip_lists(Values, Func)},
    % TODO: the name is based on the first series only
    [with_function_name(Zipped, Name)].


safe_multiply([]) -> null;
safe_multiply([{_TS, V} | Vs]) ->
    safe_multiply(Vs, V);
safe_multiply([V | Vs]) ->
    safe_multiply(Vs, V).

safe_multiply([], Acc) -> Acc;
safe_multiply([null | Vs], Acc) ->
    safe_multiply(Vs, Acc);
safe_multiply([{_TS, null} | Vs], Acc) ->
    safe_multiply(Vs, Acc);
safe_multiply([{_TS, Value} | Vs], null) ->
    safe_multiply(Vs, Value);
safe_multiply([{_TS, Value} | Vs], Acc) ->
    safe_multiply(Vs, Acc * Value);
safe_multiply([Value | Vs], null) ->
    safe_multiply(Vs, Value);
safe_multiply([Value | Vs], Acc) ->
    safe_multiply(Vs, Acc * Value).


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


square_sum([]) -> 0;
square_sum(Values) ->
    Avg = statser_calc:safe_average(Values),
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


safe_tail(_Rem, []) -> [];
safe_tail(Rem, Xs) when Rem =< 0 -> Xs;
safe_tail(Rem, [_ | Xs]) -> safe_tail(Rem - 1, Xs).


safe_last([]) -> null;
safe_last([X]) -> X;
safe_last([_ | Xs]) -> safe_last(Xs).


safe_avg(_Sum, Len) when Len =< 0 -> 0;
safe_avg(Sum, Len) -> Sum / Len.


changed(Values) ->
    lists:reverse(changed(Values, [], null)).

changed([{TS, null} | Xs], Acc, _Last) ->
    changed(Xs, [{TS, 0} | Acc], null);
changed([{TS, Val} | Xs], Acc, Val) ->
    changed(Xs, [{TS, 0} | Acc], Val);
changed([{TS, Val} | Xs], Acc, _Last) ->
    changed(Xs, [{TS, 1} | Acc], Val);
changed([], Acc, _Last) ->
    Acc.


moving_average([], _Window) -> [];
moving_average(Values, Window) when Window =< 0 -> Values;
moving_average(Values, Window) ->
    FirstPart = lists:sublist(Values, Window),
    FirstLen = statser_calc:safe_length(FirstPart),
    FirstSum = statser_calc:safe_sum(FirstPart),
    FirstAvg = safe_avg(FirstSum, FirstLen),
    Rest = safe_tail(Window, Values),
    FirstValue = get_value(safe_last(FirstPart), FirstAvg),
    lists:reverse(moving_average(Rest, Values, FirstSum, FirstLen, [FirstValue])).


moving_average([], _Last, _Sum, _Len, Acc) -> Acc;
moving_average([X | Xs], [Last | Ls], Sum, Len, Acc) ->
    {Sum0, Len0} = with_last_value(Last),
    {Sum1, Len1} = with_current_value(X),
    NewSum = Sum + Sum0 + Sum1,
    NewLen = Len + Len0 + Len1,
    Avg = safe_avg(NewSum, NewLen),
    moving_average(Xs, Ls, NewSum, NewLen, [get_value(X, Avg) | Acc]).


get_value({TS, _}, Value) -> {TS, Value};
get_value(_, Value) -> Value.


with_last_value(null) -> {0, 0};
with_last_value({_TS, null}) -> {0, 0};
with_last_value({_TS, Value}) -> {-Value, -1};
with_last_value(Value) -> {-Value, -1}.


with_current_value(null) -> {0, 0};
with_current_value({_TS, null}) -> {0, 0};
with_current_value({_TS, Value}) -> {Value, 1};
with_current_value(Value) -> {Value, 1}.


non_negative_delta(null, _Prev) -> null;
non_negative_delta(_Val, null) -> null;
non_negative_delta(Val, Prev) when Val >= Prev -> Val - Prev;
non_negative_delta(_Val, _Prev) -> null.


per_second(#series{values=Values, step=Step}) ->
    per_second(Values, Step).

per_second(Vs, Step) ->
    per_second(Vs, Step, null, []).

per_second([], _Step, _Prev, Acc) ->
    lists:reverse(Acc);
per_second([{TS, V} | Vs], Step, Prev, Acc) ->
    Value = case non_negative_delta(V, Prev) of
                null -> null;
                Diff -> Diff / Step
            end,
    per_second(Vs, Step, V, [{TS, Value} | Acc]).


non_negative_derivative(#series{values=Values}) ->
    non_negative_derivative(Values);
non_negative_derivative(Vs) ->
    non_negative_derivative(Vs, null, []).

non_negative_derivative([], _Prev, Acc) ->
    lists:reverse(Acc);
non_negative_derivative([{TS, V} | Vs], Prev, Acc) ->
    Value = non_negative_delta(V, Prev),
    non_negative_derivative(Vs, V, [{TS, Value} | Acc]).


% TODO: support limit of 'null' values to skip
keep_last_value(Values) ->
    keep_last_value(Values, null, []).

keep_last_value([], _Last, Acc) ->
    lists:reverse(Acc);
keep_last_value([{TS, null} | Vs], Last, Acc) ->
    keep_last_value(Vs, Last, [{TS, Last} | Acc]);
keep_last_value([{_TS, Val}=T | Vs], _Last, Acc) ->
    keep_last_value(Vs, Val, [T | Acc]).


filter_named(Func, Series, Name) ->
    filter_named(Func, Series, Name, []).

filter_named(Func, Series, Name, Args) ->
    lists:foldr(fun(S, Acc) ->
                        case Func(S) of
                            true -> [with_function_name(S, Name, Args) | Acc];
                            _Otherwise -> Acc
                        end
                end, [], Series).


with_function_name(Series, Name) ->
    with_function_name(Series, Name, []).

with_function_name(#series{target=Target} = Series, Name, Args) ->
    FormattedArgs = format_args(Args),
    TargetStr = binary_to_list(Target),
    % 40 = character code of '('
    % 41 = character code of ')'
    NewTarget = binary:list_to_bin(lists:flatten([Name, 40 , TargetStr] ++ FormattedArgs ++ [41])),
    Series#series{target=NewTarget}.


format_args([]) -> [];
format_args(Args) ->
    % 44 = character code of ','
    [44 | lists:join(44, Args)].

%%
%% TESTS
%%

-ifdef(TEST).


with_function_name_test_() ->
    [?_assertEqual(pseudo_target(<<"func(foo.bar)">>),
                   with_function_name(pseudo_target(<<"foo.bar">>), "func")),
     ?_assertEqual(pseudo_target(<<"func(foo.bar)">>),
                   with_function_name(pseudo_target(<<"foo.bar">>), "func", [])),
     ?_assertEqual(pseudo_target(<<"func(foo.bar,arg1,arg2)">>),
                   with_function_name(pseudo_target(<<"foo.bar">>), "func", ["arg1", "arg2"]))
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


pseudo_series_n(Values, Func) ->
    with_function_name(pseudo_series(Values), Func).


named(Series, Name) ->
    lists:map(fun(X) -> with_function_name(X, Name) end, Series).


derivative_test_() ->
    [?_assertEqual([], derivative([])),
     ?_assertEqual(pseudo_values([null,null,1,1,1,1,null,null,1,1]),
                   derivative(pseudo_values([null,1,2,3,4,5,null,6,7,8]))),
     ?_assertEqual(pseudo_values([null,1,2,2,2]),
                   derivative(pseudo_values([1,2,4,6,8])))
    ].

alias_target_test_() ->
    [?_assertEqual(pseudo_target(<<"foo">>), alias_target(pseudo_target(<<"foo">>), [])),
     ?_assertEqual(pseudo_target(<<"foo.bar">>), alias_target(pseudo_target(<<"foo.bar">>), [])),
     ?_assertEqual(pseudo_target(<<"test">>), alias_target(pseudo_target(<<"foo.bar.test">>), [2])),
     ?_assertEqual(pseudo_target(<<"test">>), alias_target(pseudo_target(<<"foo.bar.test">>), [-1])),
     ?_assertEqual(pseudo_target(<<"foo">>), alias_target(pseudo_target(<<"foo.bar.test">>), [0]))
    ].

alias_sub_test_() ->
    [?_assertEqual([pseudo_target(<<"TCP-32">>)],
                   evaluate_call(<<"aliasSub">>, [[pseudo_target(<<"foo.bar.tcp32">>)], "^.*tcp(\\d+)", "TCP-\\1"], 0, 0, 0)),
     ?_assertEqual([pseudo_target(<<"foo.bar.tcp">>)],
                   evaluate_call(<<"aliasSub">>, [[pseudo_target(<<"foo.bar.tcp">>)], "^.*tcp(\\d+)", "TCP-\\1"], 0, 0, 0))
    ].

average_above_test_() ->
    Series = [pseudo_series([5.0, 7.0])],
    Expected = [with_function_name(hd(Series), "averageAbove")],
    [?_assertEqual(Expected, evaluate_call(<<"averageAbove">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual([], evaluate_call(<<"averageAbove">>, [Series, 6.5], 0, 0, 0))
    ].

average_below_test_() ->
    Series = [pseudo_series([5.0, 7.0])],
    Expected = [with_function_name(hd(Series), "averageBelow")],
    [?_assertEqual([], evaluate_call(<<"averageBelow">>, [Series, 5.5], 0, 0, 0)),
     ?_assertEqual(Expected, evaluate_call(<<"averageBelow">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual(Expected, evaluate_call(<<"averageBelow">>, [Series, 10], 0, 0, 0))
    ].

current_above_test_() ->
    Series = [pseudo_series([5.0, 7.0])],
    Expected = [with_function_name(hd(Series), "currentAbove")],
    [?_assertEqual([], evaluate_call(<<"currentAbove">>, [Series, 8.0], 0, 0, 0)),
     ?_assertEqual(Expected, evaluate_call(<<"currentAbove">>, [Series, 4.0], 0, 0, 0)),
     ?_assertEqual(Expected, evaluate_call(<<"currentAbove">>, [Series, 6], 0, 0, 0))
    ].

current_below_test_() ->
    Series = [pseudo_series([5.0, 7.0])],
    Expected = [with_function_name(hd(Series), "currentBelow")],
    [?_assertEqual([], evaluate_call(<<"currentBelow">>, [Series, 5.0], 0, 0, 0)),
     ?_assertEqual(Expected, evaluate_call(<<"currentBelow">>, [Series, 8.0], 0, 0, 0)),
     ?_assertEqual(Expected, evaluate_call(<<"currentBelow">>, [Series, 10], 0, 0, 0))
    ].

npercentile_test_() ->
    Series = [pseudo_series([5.0, 8.0, 7.0])],
    [?_assertEqual([pseudo_series_n([7.0, 7.0, 7.0], "nPercentile")],
                   evaluate_call(<<"nPercentile">>, [Series, 50], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([5.0, 5.0, 5.0], "nPercentile")],
                   evaluate_call(<<"nPercentile">>, [Series, 10], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([8.0, 8.0, 8.0], "nPercentile")],
                   evaluate_call(<<"nPercentile">>, [Series, 99], 0, 0, 0))
    ].

average_outside_percentile_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S1 ++ S3, "averageOutsidePercentile"),
                   evaluate_call(<<"averageOutsidePercentile">>, [Series, 80], 0, 0, 0)),
     ?_assertEqual(named(Series, "averageOutsidePercentile"),
                   evaluate_call(<<"averageOutsidePercentile">>, [Series, 50], 0, 0, 0))
    ].

changed_test_() ->
    Series = [pseudo_series([1,2,2,3,3,null,3])],
    Expected = named([pseudo_series([1,1,0,1,0,0,1])], "changed"),
    [?_assertEqual(Expected, evaluate_call(<<"changed">>, [Series], 0, 0, 0))
    ].

most_deviant_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(Series, "mostDeviant"), evaluate_call(<<"mostDeviant">>, [Series, 3], 0, 0, 0)),
     ?_assertEqual(named(Series, "mostDeviant"), evaluate_call(<<"mostDeviant">>, [Series, 90], 0, 0, 0)),
     ?_assertEqual(named(S3 ++ S2, "mostDeviant"), evaluate_call(<<"mostDeviant">>, [Series, 2], 0, 0, 0)),
     ?_assertEqual(named(S3, "mostDeviant"), evaluate_call(<<"mostDeviant">>, [Series, 1], 0, 0, 0))
    ].

highest_average_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S3, "highestAverage"),
                   evaluate_call(<<"highestAverage">>, [Series, 1], 0, 0, 0)),
     ?_assertEqual(named(S3 ++ S2, "highestAverage"),
                   evaluate_call(<<"highestAverage">>, [Series, 2], 0, 0, 0)),
     ?_assertEqual(named(Series, "highestAverage"),
                   evaluate_call(<<"highestAverage">>, [Series, 3], 0, 0, 0))
    ].

highest_current_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S3, "highestCurrent"),
                   evaluate_call(<<"highestCurrent">>, [Series, 1], 0, 0, 0)),
     ?_assertEqual(named(S3 ++ S2, "highestCurrent"),
                   evaluate_call(<<"highestCurrent">>, [Series, 2], 0, 0, 0)),
     ?_assertEqual(named(Series, "highestCurrent"),
                   evaluate_call(<<"highestCurrent">>, [Series, 3], 0, 0, 0))
    ].

highest_max_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S3, "highestMax"),
                   evaluate_call(<<"highestMax">>, [Series, 1], 0, 0, 0)),
     ?_assertEqual(named(S3 ++ S2, "highestMax"),
                   evaluate_call(<<"highestMax">>, [Series, 2], 0, 0, 0)),
     ?_assertEqual(named(Series, "highestMax"),
                   evaluate_call(<<"highestMax">>, [Series, 3], 0, 0, 0))
    ].

lowest_average_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S1, "lowestAverage"),
                   evaluate_call(<<"lowestAverage">>, [Series, 1], 0, 0, 0)),
     ?_assertEqual(named(S1 ++ S2, "lowestAverage"),
                   evaluate_call(<<"lowestAverage">>, [Series, 2], 0, 0, 0)),
     ?_assertEqual(named(Series, "lowestAverage"),
                   evaluate_call(<<"lowestAverage">>, [Series, 3], 0, 0, 0))
    ].

is_non_null_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])],
    S2 = [pseudo_series([3.0, null, null])],
    [?_assertEqual(named([pseudo_series([1,1,1])], "isNonNull"),
                   evaluate_call(<<"isNonNull">>, [S1], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([1,0,0])], "isNonNull"),
                   evaluate_call(<<"isNonNull">>, [S2], 0, 0, 0))
    ].

lowest_current_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])], % avg 4.0
    S2 = [pseudo_series([3.0, 9.0, 6.0])], % avg 6.0
    S3 = [pseudo_series([3.0, 12.0, 9.0])], % avg 8.0
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S1, "lowestCurrent"),
                   evaluate_call(<<"lowestCurrent">>, [Series, 1], 0, 0, 0)),
     ?_assertEqual(named(S1 ++ S2, "lowestCurrent"),
                   evaluate_call(<<"lowestCurrent">>, [Series, 2], 0, 0, 0)),
     ?_assertEqual(named(Series, "lowestCurrent"),
                   evaluate_call(<<"lowestCurrent">>, [Series, 3], 0, 0, 0))
    ].

keep_last_value_test_() ->
    S1 = [pseudo_series([1,2,4])],
    S2 = [pseudo_series([2,null,null,4])],
    S3 = [pseudo_series([null,2,null, null])],
    [?_assertEqual(S1, evaluate_call(<<"keepLastValue">>, [S1], 0, 0, 0)),
     ?_assertEqual([pseudo_series([2,2,2,4])],
                   evaluate_call(<<"keepLastValue">>, [S2], 0, 0, 0)),
     ?_assertEqual([pseudo_series([null,2,2,2])],
                   evaluate_call(<<"keepLastValue">>, [S3], 0, 0, 0))
    ].

maximum_above_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])],
    S2 = [pseudo_series([3.0, 9.0, 6.0])],
    S3 = [pseudo_series([3.0, 12.0, 9.0])],
    S4 = [pseudo_series([null, null, null])],
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S3, "maximumAbove"),
                   evaluate_call(<<"maximumAbove">>, [Series, 10], 0, 0, 0)),
     ?_assertEqual(named(S2 ++ S3, "maximumAbove"),
                   evaluate_call(<<"maximumAbove">>, [Series, 8], 0, 0, 0)),
     ?_assertEqual(named(Series, "maximumAbove"),
                   evaluate_call(<<"maximumAbove">>, [Series, 4], 0, 0, 0)),
     ?_assertEqual(named(Series, "maximumAbove"),
                   evaluate_call(<<"maximumAbove">>, [Series ++ S4, 4], 0, 0, 0))
    ].

maximum_below_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])],
    S2 = [pseudo_series([3.0, 9.0, 6.0])],
    S3 = [pseudo_series([3.0, 12.0, 9.0])],
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S1, "maximumBelow"),
                   evaluate_call(<<"maximumBelow">>, [Series, 6], 0, 0, 0)),
     ?_assertEqual(named(S1 ++ S2, "maximumBelow"),
                   evaluate_call(<<"maximumBelow">>, [Series, 10], 0, 0, 0)),
     ?_assertEqual(named(Series, "maximumBelow"),
                   evaluate_call(<<"maximumBelow">>, [Series, 13], 0, 0, 0))
    ].

minimum_above_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])],
    S2 = [pseudo_series([5.0, 9.0, 6.0])],
    S3 = [pseudo_series([7.0, 12.0, 9.0])],
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S3, "minimumAbove"),
                   evaluate_call(<<"minimumAbove">>, [Series, 6], 0, 0, 0)),
     ?_assertEqual(named(S2 ++ S3, "minimumAbove"),
                   evaluate_call(<<"minimumAbove">>, [Series, 4], 0, 0, 0)),
     ?_assertEqual(named(Series, "minimumAbove"),
                   evaluate_call(<<"minimumAbove">>, [Series, 2], 0, 0, 0))
    ].

minimum_below_test_() ->
    S1 = [pseudo_series([3.0, 5.0, 4.0])],
    S2 = [pseudo_series([5.0, 9.0, 6.0])],
    S3 = [pseudo_series([7.0, 12.0, 9.0])],
    Series = S1 ++ S2 ++ S3,
    [?_assertEqual(named(S1, "minimumBelow"),
                   evaluate_call(<<"minimumBelow">>, [Series, 4], 0, 0, 0)),
     ?_assertEqual(named(S1 ++ S2, "minimumBelow"),
                   evaluate_call(<<"minimumBelow">>, [Series, 6], 0, 0, 0)),
     ?_assertEqual(named(Series, "minimumBelow"),
                   evaluate_call(<<"minimumBelow">>, [Series, 8], 0, 0, 0))
    ].

exclude_test_() ->
    Series = [pseudo_series([1,2,4])],
    [?_assertEqual(Series, evaluate_call(<<"exclude">>, [Series, "none"], 0, 0, 0)),
     ?_assertEqual([], evaluate_call(<<"exclude">>, [Series, "target"], 0, 0, 0)),
     ?_assertEqual([], evaluate_call(<<"exclude">>, [Series, "get$"], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"exclude">>, [Series, "^taget$"], 0, 0, 0))
    ].

grep_test_() ->
    Series = [pseudo_series([1,2,4])],
    [?_assertEqual([], evaluate_call(<<"grep">>, [Series, "none"], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"grep">>, [Series, "target"], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"grep">>, [Series, "get$"], 0, 0, 0)),
     ?_assertEqual([], evaluate_call(<<"grep">>, [Series, "^taget$"], 0, 0, 0))
    ].

as_percent_test_() ->
    S1 = [pseudo_series([10,20,30,40,50])],
    S2 = [pseudo_series([10,null,30,40,50])],
    [?_assertEqual([pseudo_series_n([10.0,20.0,30.0,40.0,50.0], "asPercent")],
                   evaluate_call(<<"asPercent">>, [S1, 100], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([5.0,10.0,15.0,20.0,25.0], "asPercent")],
                   evaluate_call(<<"asPercent">>, [S1, 200], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([5.0,null,15.0,20.0,25.0], "asPercent")],
                   evaluate_call(<<"asPercent">>, [S2, 200], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([50.0,50.0,50.0,50.0,50.0], "asPercent"),
                    pseudo_series_n([50.0,50.0,50.0,50.0,50.0], "asPercent")],
                   evaluate_call(<<"asPercent">>, [S1 ++ S1], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([50.0,50.0,50.0,50.0,50.0], "asPercent"),
                    pseudo_series_n([50.0,50.0,50.0,50.0,50.0], "asPercent")],
                   evaluate_call(<<"asPercent">>, [S1 ++ S1, S1 ++ S1], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([100.0,100.0,100.0,100.0,100.0], "asPercent")],
                   evaluate_call(<<"asPercent">>, [S1, S1], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([100.0,100.0,100.0,100.0,100.0], "asPercent")],
                   evaluate_call(<<"asPercent">>, [S1], 0, 0, 0))
    ].

integral_test_() ->
    [?_assertEqual([pseudo_series_n([1,2,3], "integral")],
                   evaluate_call(<<"integral">>, [[pseudo_series([1,1,1])]], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([1,null,2,3], "integral")],
                   evaluate_call(<<"integral">>, [[pseudo_series([1,null,1,1])]], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([], "integral")],
                   evaluate_call(<<"integral">>, [[pseudo_series([])]], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([null,null], "integral")],
                   evaluate_call(<<"integral">>, [[pseudo_series([null,null])]], 0, 0, 0))
    ].

integral_by_interval_test_() ->
    S1 = [pseudo_series([1,2,3,4,5,6,7])],
    S2 = [pseudo_series([1,null,1,1,null])],
    S3 = [pseudo_series([null,null,null])],
    [?_assertEqual([pseudo_series_n([1,3,6,4,9,15,7], "integralByInterval")],
                   evaluate_call(<<"integralByInterval">>, [S1, 3], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([1,2,3,4,5,6,7], "integralByInterval")],
                   evaluate_call(<<"integralByInterval">>, [S1, <<"5s">>], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([1,2,3,4,5,6,7], "integralByInterval")],
                   evaluate_call(<<"integralByInterval">>, [S1, <<"10s">>], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([], "integralByInterval")],
                   evaluate_call(<<"integralByInterval">>, [[pseudo_series([])], 1], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([1,1,1,2,0], "integralByInterval")],
                   evaluate_call(<<"integralByInterval">>, [S2, 2], 0, 0, 0)),
     ?_assertEqual([pseudo_series_n([0,0,0], "integralByInterval")],
                   evaluate_call(<<"integralByInterval">>, [S3, <<"20s">>], 0, 0, 0))
    ].

sum_series_test_() ->
    Series1 = [pseudo_series([1,2,3])],
    Series2 = [pseudo_series([1,2,3], 20)],
    Series3 = [pseudo_series([1,1,2,2,3,3])],
    [?_assertEqual(named([pseudo_series([2,4,6])], "sumSeries"),
                   evaluate_call(<<"sumSeries">>, [Series1, Series1], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([2,4,6])], "sumSeries"),
                   evaluate_call(<<"sumSeries">>, [[Series1 ++ Series1]], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([2.0,4.0,6.0], 20)], "sumSeries"),
                   evaluate_call(<<"sumSeries">>, [[Series2 ++ Series3]], 0, 0, 0))
    ].

stddev_series_test_() ->
    Series1 = [pseudo_series([1,2,3])],
    Series2 = [pseudo_series([2,4,6])],
    Series3 = [pseudo_series([3,6,9])],
    Ss = Series1 ++ Series2 ++ Series3,
    Expected = [math:sqrt(2/3),math:sqrt(8/3),math:sqrt(18/3)],
    [?_assertEqual(named([pseudo_series(Expected)], "stddevSeries"),
                   evaluate_call(<<"stddevSeries">>, [Ss], 0, 0, 0))
    ].

divide_series_test_() ->
    Series1 = [pseudo_series([10,20,30])],
    Series2 = [pseudo_series([2,2,3])],
    [?_assertEqual(named([pseudo_series([5.0,10.0,10.0])], "divideSeries"),
                   evaluate_call(<<"divideSeries">>, [Series1, Series2], 0, 0, 0))
    ].

max_series_test_() ->
    S1 = [pseudo_series([null,1,2,4,3])],
    S2 = [pseudo_series([1,2,1,null,8])],
    [?_assertEqual(named([pseudo_series([1,2,2,4,8])], "maxSeries"),
                   evaluate_call(<<"maxSeries">>, [[S1 ++ S2]], 0, 0, 0)),
     ?_assertEqual(named(S1, "maxSeries"),
                   evaluate_call(<<"maxSeries">>, [[S1 ++ S1]], 0, 0, 0))
    ].

min_series_test_() ->
    S1 = [pseudo_series([null,1,2,4,3])],
    S2 = [pseudo_series([1,2,1,null,8])],
    [?_assertEqual(named([pseudo_series([1,1,1,4,3])], "minSeries"),
                   evaluate_call(<<"minSeries">>, [[S1 ++ S2]], 0, 0, 0)),
     ?_assertEqual(named(S1, "minSeries"),
                   evaluate_call(<<"minSeries">>, [[S1 ++ S1]], 0, 0, 0))
    ].

multiply_series_test_() ->
    Series = [pseudo_series([null,1,2,null,3])],
    [?_assertEqual(named([pseudo_series([null, 1, 4, null, 9])], "multiplySeries"),
                   evaluate_call(<<"multiplySeries">>, [Series, Series], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([null, 1, 4, null, 9])], "multiplySeries"),
                   evaluate_call(<<"multiplySeries">>, [[Series ++ Series]], 0, 0, 0))
    ].

pow_test_() ->
    Series = [pseudo_series([1,null,2,4,1,2])],
    [?_assertEqual(named([pseudo_series([1.0,null,8.0,64.0,1.0,8.0])], "pow"),
                   evaluate_call(<<"pow">>, [Series, 3], 0, 0, 0))
    ].

pow_series_test_() ->
    S1 = [pseudo_series([null,1,2,4,3])],
    S2 = [pseudo_series([1,2,1,null,8])],
    [?_assertEqual(named([pseudo_series([null,1.0,2.0,null,math:pow(3, 8)])], "powSeries"),
                   evaluate_call(<<"powSeries">>, [[S1 ++ S2]], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([null,1.0,4.0,math:pow(4, 4),27.0])], "powSeries"),
                   evaluate_call(<<"powSeries">>, [[S1 ++ S1]], 0, 0, 0))
    ].

range_of_series_test_() ->
    S1 = [pseudo_series([null,1,2,4,3])],
    S2 = [pseudo_series([1,2,1,null,8])],
    [?_assertEqual(named([pseudo_series([0,1,1,0,5])], "rangeOfSeries"),
                   evaluate_call(<<"rangeOfSeries">>, [[S1 ++ S2]], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([null,0,0,0,0])], "rangeOfSeries"),
                   evaluate_call(<<"rangeOfSeries">>, [[S1 ++ S1]], 0, 0, 0))
    ].

average_series_test_() ->
    Series1 = [pseudo_series([1,1,2])],
    Series2 = [pseudo_series([2,3,4])],
    Series3 = [pseudo_series([3,null,9])],
    [?_assertEqual(named([pseudo_series([2.0,2.0,5.0])], "averageSeries"),
                   evaluate_call(<<"avg">>, [Series1, Series2, Series3], 0, 0, 0))
    ].

diff_series_test_() ->
    Series1 = [pseudo_series([1,2,3])],
    Series2 = [pseudo_series([3,2,6])],
    [?_assertEqual(named([pseudo_series([-2,0,-3])], "diffSeries"),
                   evaluate_call(<<"diffSeries">>, [Series1, Series2], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([-2,0,-3])], "diffSeries"),
                   evaluate_call(<<"diffSeries">>, [[Series1 ++ Series2]], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([-5,-2,-9])], "diffSeries"),
                   evaluate_call(<<"diffSeries">>, [[Series1 ++ Series2 ++ Series2]], 0, 0, 0))
    ].

offset_to_zero_test_() ->
    Series = [pseudo_series([102, 101, 104, 101, 100, 111])],
    Expected = [pseudo_series_n([2, 1, 4, 1, 0, 11], "offsetToZero")],
    [?_assertEqual(Expected, evaluate_call(<<"offsetToZero">>, [Series], 0, 0, 0))].

scale_test_() ->
    Series = [pseudo_series([1,2,3,4])],
    Expected = [pseudo_series_n([2,4,6,8], "scale")],
    [?_assertEqual(Expected, evaluate_call(<<"scale">>, [Series, 2], 0, 0, 0))].

scale_to_seconds_test_() ->
    Series = [pseudo_series([1,2,4,4])],
    Exp1 = [pseudo_series_n([1/10,2/10,4/10,4/10], "scaleToSeconds")],
    Exp2 = [pseudo_series_n([1*6.0,2*6.0,4*6.0,4*6.0], "scaleToSeconds")],
    [?_assertEqual(Exp1, evaluate_call(<<"scaleToSeconds">>, [Series, 1], 0, 0, 0)),
     ?_assertEqual(Exp2, evaluate_call(<<"scaleToSeconds">>, [Series, 60], 0, 0, 0))
    ].

per_second_test_() ->
    S1 = pseudo_series([10, 20, 25, 30, 40, 60]),
    S2 = pseudo_series([10, 20, null, 30, 40, null], 20),
    [?_assertEqual(pseudo_values([null, 10/10, 5/10, 5/10, 10/10, 20/10]), per_second(S1)),
     ?_assertEqual(pseudo_values([null, 10/20, null, null, 10/20, null], 20), per_second(S2)),
     ?_assertEqual(named([pseudo_series([null, 10/10, 5/10, 5/10, 10/10, 20/10])], "perSecond"),
                   evaluate_call(<<"perSecond">>, [[S1]], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([null, 10/20, null, null, 10/20, null], 20)], "perSecond"),
                   evaluate_call(<<"perSecond">>, [[S2]], 0, 0, 0))
    ].

non_negative_derivative_test_() ->
    S1 = pseudo_series([10, 20, 25, 30, 40, 60]),
    S2 = pseudo_series([10, 20, null, 30, 40, null]),
    [?_assertEqual(pseudo_values([null, 10, 5, 5, 10, 20]), non_negative_derivative(S1)),
     ?_assertEqual(pseudo_values([null, 10, null, null, 10, null]), non_negative_derivative(S2)),
     ?_assertEqual(named([pseudo_series([null, 10, 5, 5, 10, 20])], "nonNegativeDerivative"),
                   evaluate_call(<<"nonNegativeDerivative">>, [[S1]], 0, 0, 0)),
     ?_assertEqual(named([pseudo_series([null, 10, null, null, 10, null])], "nonNegativeDerivative"),
                   evaluate_call(<<"nonNegativeDerivative">>, [[S2]], 0, 0, 0))
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

safe_multiply_test_() ->
    [?_assertEqual(null, safe_multiply([])),
     ?_assertEqual(null, safe_multiply([null, null])),
     ?_assertEqual(3, safe_multiply([null, 3, null])),
     ?_assertEqual(6, safe_multiply([3, 2])),
     ?_assertEqual(6, safe_multiply([null, 3, null, 2, null])),
     ?_assertEqual(6, safe_multiply(pseudo_values([3, 2]))),
     ?_assertEqual(6, safe_multiply(pseudo_values([null, 3, null, 2, null])))
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
    Normalize = fun(S) -> {N, _, _, _} = normalize(S), N end,
    Series = [pseudo_series([1,1,1], 20), pseudo_series([2,2,2,2,2,2])],
    [?_assertEqual([pseudo_series([1,1,1], 20), pseudo_series([2.0,2.0,2.0], 20)], Normalize([Series]))].

zip_lists_test_() ->
    [?_assertEqual([], zip_lists([], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3], [1,2,3]], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3,4], [1,2,3]], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3], [1,2,3,4]], fun lists:sum/1)),
     ?_assertEqual([2, 4, 6], zip_lists([[1,2,3], [1,2,3,4,5,6,7]], fun lists:sum/1)),
     ?_assertEqual([3, 6, 9], zip_lists([[1,2,3], [1,2,3], [1,2,3]], fun lists:sum/1))
    ].

moving_average_test_() ->
    [?_assertEqual([], moving_average([], 3)),
     ?_assertEqual([6/3,9/3,10/3,9/3,6/3,4/3,3/3], moving_average([1,2,3,4,3,2,1,1,1], 3)),
     ?_assertEqual([6/3], moving_average([1,4,1], 3)),
     ?_assertEqual([6/3], moving_average([1,4,1], 4)),
     ?_assertEqual([1/1,4/1,1/1], moving_average([1,4,1], 1)),
     ?_assertEqual([5/2,9/3,10/3,9/3,6/3,4/3,2/2], moving_average([null,2,3,4,3,2,1,1,null], 3)),
     ?_assertEqual([{120,6/3},{130,9/3},{140,10/3},{150,9/3},{160,6/3},{170,4/3},{180,3/3}],
                   moving_average(pseudo_values([1,2,3,4,3,2,1,1,1]), 3))
    ].

-endif.
