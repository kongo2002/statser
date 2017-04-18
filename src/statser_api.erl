-module(statser_api).
-export([handle/2, handle_event/3]).

-include_lib("elli/include/elli.hrl").
-behaviour(elli_handler).

handle(Req, _Args) ->
    handle(Req#req.method, elli_request:path(Req), Req).


% render API
handle('POST', [<<"render">> | _], Req) ->
    Args = elli_request:post_args_decoded(Req),
    Targets = many_by_key(<<"target">>, Args),
    % 'from' defaults to -24 h
    From = get_or_fallback(<<"from">>, Args, <<"-1d">>),
    % 'until' defaults to now
    Until = get_or_fallback(<<"until">>, Args, <<"now">>),
    MaxPoints = get_or_fallback(<<"maxDataPoints">>, Args, 366),

    handle_render(Targets, From, Until, MaxPoints);

handle(_, _, _Req) ->
    {404, [], <<"not found">>}.


%% @doc: Handle request events, like request completed, exception
%% thrown, client timeout, etc. Must return 'ok'.
handle_event(_Event, _Data, _Args) ->
    ok.


handle_render([], _From, _Until, _MaxPoints) ->
    {400, [], <<"no target(s) specified">>};
handle_render(_Targets, false, _Until, _MaxPoints) ->
    {400, [], <<"no 'from' specified">>};
handle_render(_Targets, _From, false, _MaxPoints) ->
    {400, [], <<"no 'until' specified">>};
handle_render(Targets, From, Until, MaxPoints) ->
    lager:debug("render request ~p [~p - ~p] [~w]", [Targets, From, Until, MaxPoints]),
    {ok, [], <<"not implemented yet">>}.


many_by_key(Key, List) ->
    many_by_key(Key, List, []).

many_by_key(_Key, [], Acc) -> Acc;
many_by_key(Key, [{Key, Value} | Rest], Acc) ->
    many_by_key(Key, Rest, [Value | Acc]);
many_by_key(Key, [_Otherwise | Rest], Acc) ->
    many_by_key(Key, Rest, Acc).

get_or_fallback(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        false -> Default;
        {Key, Value} -> Value
    end.

get_by_key(Key, List) -> get_or_fallback(Key, List, false).
