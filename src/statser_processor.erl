-module(statser_processor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
                              {Path, Result};
                          [{_Path, Pid}] ->
                              Result = gen_server:call(Pid, {fetch, From, Until, Now}, ?TIMEOUT),
                              {Path, Result}
                      end
              end, Paths).


% absolute
evaluate_call(<<"absolute">>, [Series], _From, _Until, _Now) ->
    lists:map(fun({Target, Values}) ->
                      {Target, process_series_values(Values, fun erlang:abs/1)}
              end, Series);

% alias
evaluate_call(<<"alias">>, [Series, Alias], _From, _Until, _Now) when is_binary(Alias) ->
    lists:map(fun({_Target, Values}) -> {Alias, Values} end, Series);

% aliasByMetric
evaluate_call(<<"aliasByMetric">>, [Series], _From, _Until, _Now) ->
    lists:map(fun({Target, Values}) ->
                      {alias_target(Target, [-1]), Values}
              end, Series);

% aliasByNode
evaluate_call(<<"aliasByNode">>, [Series | Aliases], _From, _Until, _Now) ->
    lists:map(fun({Target, Values}) ->
                      {alias_target(Target, Aliases), Values}
              end, Series);

% averageAbove
evaluate_call(<<"averageAbove">>, [Series, Avg], _From, _Until, _Now) ->
    lists:filter(fun({_, Values}) -> safe_average(Values) >= Avg end, Series);

% averageBelow
evaluate_call(<<"averageBelow">>, [Series, Avg], _From, _Until, _Now) ->
    lists:filter(fun({_, Values}) -> safe_average(Values) =< Avg end, Series);

% derivative
evaluate_call(<<"derivative">>, [Series], _From, _Until, _Now) ->
    lists:map(fun({Target, Values}) ->
                      {Target, derivative(Values)}
              end, Series);

evaluate_call(Unknown, _Args, _From, _Until, _Now) ->
    lager:error("unknown function call ~p or invalid arguments", [Unknown]),
    error.


process_series_values(Series, Func) ->
    lists:map(fun ({_, null} = T) -> T;
                  ({TS, Value}) -> {TS, Func(Value)}
              end, Series).


alias_target(Target, []) -> Target;
alias_target(Target, Aliases) ->
    Parts = binary:split(Target, <<".">>, [global, trim_all]),
    NumParts = length(Parts),
    to_target(lists:map(fun(Idx) -> get_part(Parts, NumParts, Idx) end, Aliases)).


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


%%
%% TESTS
%%

-ifdef(TEST).

alias_target_test_() ->
    [?_assertEqual(<<"foo">>, alias_target(<<"foo">>, [])),
     ?_assertEqual(<<"foo.bar">>, alias_target(<<"foo.bar">>, [])),
     ?_assertEqual(<<"test">>, alias_target(<<"foo.bar.test">>, [2])),
     ?_assertEqual(<<"test">>, alias_target(<<"foo.bar.test">>, [-1])),
     ?_assertEqual(<<"foo">>, alias_target(<<"foo.bar.test">>, [0]))
    ].

pseudo_series(Series) ->
    lists:map(fun(V) -> {100, V} end, Series).

derivative_test_() ->
    [?_assertEqual([], derivative([])),
     ?_assertEqual(pseudo_series([null,null,1,1,1,1,null,null,1,1]),
                   derivative(pseudo_series([null,1,2,3,4,5,null,6,7,8]))),
     ?_assertEqual(pseudo_series([null,1,2,2,2]),
                   derivative(pseudo_series([1,2,4,6,8])))
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
    Series = [{<<"target">>, [{100, 5.0}, {110, 7.0}]}],
    [?_assertEqual(Series, evaluate_call(<<"averageAbove">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual([], evaluate_call(<<"averageAbove">>, [Series, 6.5], 0, 0, 0))
    ].

average_below_test_() ->
    Series = [{<<"target">>, [{100, 5.0}, {110, 7.0}]}],
    [?_assertEqual([], evaluate_call(<<"averageBelow">>, [Series, 5.5], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageBelow">>, [Series, 6.0], 0, 0, 0)),
     ?_assertEqual(Series, evaluate_call(<<"averageBelow">>, [Series, 10], 0, 0, 0))
    ].

-endif.
