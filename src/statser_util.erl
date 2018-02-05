-module(statser_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-compile({no_auto_import, [floor/1]}).

-export([ceiling/1,
         floor/1,
         to_number/1,
         parse_unit/1,
         parse_unit/2,
         seconds/0,
         epoch_seconds_to_datetime/1]).


to_number(Binary) ->
    List = binary_to_list(Binary),
    case string:to_float(List) of
        {error, no_float} ->
            case string:to_integer(List) of
                {error, _} -> error;
                {Result, _} -> {ok, Result}
            end;
        {Result, _} -> {ok, Result}
    end.


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


parse_unit(Value) ->
    List = binary_to_list(Value),
    case string:to_integer(List) of
        {error, _} -> error;
        {Val, Unit} -> parse_unit(Val, Unit)
    end.


parse_unit(Value, [$s | _])   -> Value;
parse_unit(Value, [$S | _])   -> Value;
parse_unit(Value, "min" ++ _) -> Value * 60;
parse_unit(Value, [$h | _])   -> Value * 3600;
parse_unit(Value, [$d | _])   -> Value * 86400;
parse_unit(Value, [$w | _])   -> Value * 604800;
parse_unit(Value, "mon" ++ _) -> Value * 2592000;
parse_unit(Value, [$y | _])   -> Value * 31536000;
parse_unit(_, _)              -> error.


seconds() ->
    erlang:system_time(second).


epoch_seconds_to_datetime(Seconds) ->
    % calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0})
    EpochSeconds = 62167219200,
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:gregorian_seconds_to_datetime(EpochSeconds + Seconds),
    lists:flatten(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",
                                [Year,Month,Day,Hour,Minute,Second])).


%%
%% TESTS
%%

-ifdef(TEST).

parse_unit_test_() ->
    [?_assertEqual(error, parse_unit(100, "")),
     ?_assertEqual(error, parse_unit(100, "m")),
     ?_assertEqual(100, parse_unit(100, "s")),
     ?_assertEqual(180, parse_unit(3, "min")),
     ?_assertEqual(2 * 86400 * 7, parse_unit(2, "w"))
    ].

floor_test_() ->
    [?_assertEqual(5, floor(5.0)),
     ?_assertEqual(5, floor(5)),
     ?_assertEqual(5, floor(5.5)),
     ?_assertEqual(5, floor(5.9)),
     ?_assertEqual(-6, floor(-6)),
     ?_assertEqual(-6, floor(-5.1)),
     ?_assertEqual(-6, floor(-5.9))
    ].

ceiling_test_() ->
    [?_assertEqual(5, ceiling(5.0)),
     ?_assertEqual(5, ceiling(5)),
     ?_assertEqual(6, ceiling(5.5)),
     ?_assertEqual(6, ceiling(5.9)),
     ?_assertEqual(-5, ceiling(-5)),
     ?_assertEqual(-5, ceiling(-5.1)),
     ?_assertEqual(-5, ceiling(-5.9))
    ].

-endif.
