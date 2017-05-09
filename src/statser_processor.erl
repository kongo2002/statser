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

-endif.
