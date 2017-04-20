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
    Format = get_or_fallback(<<"format">>, Args, <<"json">>),

    case Format of
        <<"json">> ->
            handle_render(Targets, From, Until, MaxPoints);
        Unsupported ->
            {400, [], <<"unsupported format '", Unsupported/binary, "'">>}
    end;

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
    Processed = lists:map(fun(Target) -> process_target(Target, From, Until, MaxPoints) end, Targets),
    {ok, [], <<"not implemented yet">>}.


process_target(Target, From, Until, MaxPoints) ->
    lager:debug("render request for target ~p [~p - ~p] [~w]", [Target, From, Until, MaxPoints]),
    Parsed = statser_parser:parse(Target),
    Parameters = {From, Until, MaxPoints},
    process(Parsed, Parameters).


process({paths, Paths}, Params) -> process_paths(Paths, Params);
process({call, Fctn, Args}, Params) -> process_function(Fctn, Args, Params);
process({template, Expr, Args}, Params) -> process_template(Expr, Args, Params);
process(Invalid, _Params) ->
    lager:error("failed to parse target expression: ~p", [Invalid]),
    error.


process_paths(Paths, {From, Until, MaxPoints} = Params) ->
    [{0, 0}].


process_function(Fctn, Args, {From, Until, MaxPoints} = Params) ->
    [{0, 0}].


process_template(Expr, Args, {From, Until, MaxPoints} = Params) ->
    [{0, 0}].


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
