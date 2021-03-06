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

-module(statser_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-compile({no_auto_import, [floor/1]}).

-export([ceiling/1,
         floor/1,
         to_number/1,
         number_to_bin/1,
         parse_unit/1,
         parse_unit/2,
         seconds/0,
         epoch_seconds_to_datetime/1,
         split_metric/1]).


-spec number_to_bin(number()) -> binary().
number_to_bin(Num) when is_integer(Num) ->
    list_to_binary(integer_to_list(Num));
number_to_bin(Num) ->
    list_to_binary(io_lib:format("~.2f", [Num])).


-spec to_number(binary()) -> {ok, number()} | error.
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


-spec floor(number()) -> integer().
floor(X) when X < 0 ->
    Truncated = trunc(X),
    case X - Truncated == 0 of
       true -> Truncated;
       false -> Truncated - 1
    end;
floor(X) ->
    trunc(X).


-spec ceiling(number()) -> integer().
ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    Truncated = trunc(X),
    case X - Truncated == 0 of
       true -> Truncated;
       false -> Truncated + 1
    end.


-spec parse_unit(binary()) -> integer() | error.
parse_unit(Value) ->
    List = binary_to_list(Value),
    case string:to_integer(List) of
        {error, _} -> error;
        {Val, Unit} -> parse_unit(Val, Unit)
    end.


-spec parse_unit(integer(), unicode:chardata()) -> integer() | error.
parse_unit(Value, [$s | _])   -> Value;
parse_unit(Value, [$S | _])   -> Value;
parse_unit(Value, "min" ++ _) -> Value * 60;
parse_unit(Value, [$h | _])   -> Value * 3600;
parse_unit(Value, [$d | _])   -> Value * 86400;
parse_unit(Value, [$w | _])   -> Value * 604800;
parse_unit(Value, "mon" ++ _) -> Value * 2592000;
parse_unit(Value, [$y | _])   -> Value * 31536000;
parse_unit(_, _)              -> error.


-spec seconds() -> integer().
seconds() ->
    erlang:system_time(second).


-spec epoch_seconds_to_datetime(integer()) -> string().
epoch_seconds_to_datetime(Seconds) ->
    % calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0})
    EpochSeconds = 62167219200,
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:gregorian_seconds_to_datetime(EpochSeconds + Seconds),
    lists:flatten(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",
                                [Year, Month, Day, Hour, Minute, Second])).


-spec split_metric(binary()) -> [binary()].
split_metric(Metric) ->
    binary:split(Metric, <<".">>, [global, trim_all]).


%%
%% TESTS
%%

-ifdef(TEST).

number_to_bin_test_() ->
    [?_assertEqual(<<"325">>, number_to_bin(325)),
     ?_assertEqual(<<"-35.21">>, number_to_bin(-35.21))
    ].

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

epoch_seconds_to_datetime_test_() ->
    [?_assertEqual("1970-01-01T00:00:00", epoch_seconds_to_datetime(0)),
     ?_assertEqual("1970-01-01T01:00:00", epoch_seconds_to_datetime(3600)),
     ?_assertEqual("1970-01-01T01:01:01", epoch_seconds_to_datetime(3661))
    ].

-endif.
