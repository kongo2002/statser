-module(statser_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([parse_formdata/1]).

% percent ansi char code
-define(PERCENT, 37).

% determine whether hex code char
-define(IS_ENCODED(C), ((C >= $0 andalso C =< $9) orelse
                        (C >= $a andalso C =< $f) orelse
                        (C >= $A andalso C =< $F))).

parse_formdata(Bin) when is_binary(Bin) ->
    parse_formdata(binary_to_list(Bin));
parse_formdata(String) ->
    parse_formdata(String, []).

parse_formdata([], Acc) ->
    lists:reverse(Acc);
parse_formdata(String, Acc) ->
    {Key, Rest0} = parse_key(String),
    {Value, Rest} = parse_value(Rest0),
    parse_formdata(Rest, [{Key, Value} | Acc]).


parse_key(String) ->
    parse_key(String, []).

parse_key([], Acc) ->
    {reverse_decode(Acc), ""};
parse_key([$= | Rest], Acc) ->
    {reverse_decode(Acc), Rest};
parse_key(Rest=[$; | _], Acc) ->
    {reverse_decode(Acc), Rest};
parse_key(Rest=[$& | _], Acc) ->
    {reverse_decode(Acc), Rest};
parse_key([C | Rest], Acc) ->
    parse_key(Rest, [C | Acc]).


parse_value(String) ->
    parse_value(String, []).

parse_value([], Acc) ->
    {reverse_decode(Acc), ""};
parse_value([$; | Rest], Acc) ->
    {reverse_decode(Acc), Rest};
parse_value([$& | Rest], Acc) ->
    {reverse_decode(Acc), Rest};
parse_value([C | Rest], Acc) ->
    parse_value(Rest, [C | Acc]).


reverse_decode(S) ->
    reverse_decode(S, []).

reverse_decode([], Acc) ->
    Acc;
reverse_decode([$+ | Rest], Acc) ->
    reverse_decode(Rest, [$\s | Acc]);
reverse_decode([Lo, Hi, ?PERCENT | Rest], Acc) when ?IS_ENCODED(Lo), ?IS_ENCODED(Hi) ->
    reverse_decode(Rest, [(from_hex(Lo) bor (from_hex(Hi) bsl 4)) | Acc]);
reverse_decode([C | Rest], Acc) ->
reverse_decode(Rest, [C | Acc]).


from_hex(C) when C >= $0, C =< $9 -> C - $0;
from_hex(C) when C >= $a, C =< $f -> C - $a + 10;
from_hex(C) when C >= $A, C =< $F -> C - $A + 10.


%%
%% TESTS
%%

-ifdef(TEST).

parse_formdata_test_() ->
    [?_assertEqual([], parse_formdata("")),
     ?_assertEqual([], parse_formdata(<<"">>)),
     ?_assertEqual([{"foo", "bar"}], parse_formdata(<<"foo=bar">>)),
     ?_assertEqual([{"foo", "2393"}], parse_formdata(<<"foo=2393">>)),
     ?_assertEqual([{"foo", "ham eggs"}], parse_formdata(<<"foo=ham+eggs">>)),
     ?_assertEqual([{"foo", "ham"}, {"foo", "eggs"}], parse_formdata(<<"foo=ham&foo=eggs">>))
    ].

-endif.
