-module(statser_api).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([handle/2, handle_event/3]).

-include_lib("elli/include/elli.hrl").
-behaviour(elli_handler).

-define(NO_CACHE, <<"no-cache">>).
-define(DEFAULT_HEADERS, [{<<"Pragma">>, ?NO_CACHE},
                          {<<"Cache-Control">>, ?NO_CACHE},
                          {<<"Content-Type">>, <<"application/json; charset=utf-8">>},
                          {<<"Access-Control-Allow-Origin">>, <<"*">>}]).

handle(Req, _Args) ->
    handle(Req#req.method, elli_request:path(Req), Req).

% metrics API
handle('GET', [<<"metrics">>], Req) ->
    handle_metrics(Req);

handle('GET', [<<"metrics">>, <<"find">>], Req) ->
    handle_metrics(Req);

% render API
handle('POST', [<<"render">>], Req) ->
    Args = elli_request:post_args_decoded(Req),
    Targets = many_by_key(<<"target">>, Args),
    % 'from' defaults to -24 h
    From0 = get_or_fallback(<<"from">>, Args, <<"-1d">>),
    % 'until' defaults to now
    Until0 = get_or_fallback(<<"until">>, Args, <<"now">>),
    MaxPoints = get_or_fallback(<<"maxDataPoints">>, Args, 366),
    Format = get_or_fallback(<<"format">>, Args, <<"json">>),

    case Format of
        <<"json">> ->
            Now = erlang:system_time(second),
            From = parse_time(From0, Now),
            Until = parse_time(Until0, Now),
            handle_render(Targets, From, Until, MaxPoints);
        Unsupported ->
            {400, [], <<"unsupported format '", Unsupported/binary, "'">>}
    end;

handle(_, _, _Req) ->
    {404, [], <<"not found">>}.


handle_event(request_throw, [Req, Exception, Stack], _Config) ->
    lager:error("exception: ~p stack: ~p request: ~p",
                [Exception, Stack, elli_request:to_proplist(Req)]),
    ok;
handle_event(request_exit, [Req, Exit, Stack], _Config) ->
    lager:error("exit: ~p stack: ~p request: ~p",
                [Exit, Stack, elli_request:to_proplist(Req)]),
    ok;

handle_event(request_error, [Req, Error, Stack], _Config) ->
    lager:error("error: ~p stack: ~p request: ~p",
                [Error, Stack, elli_request:to_proplist(Req)]),
    ok;

handle_event(_Event, _Data, _Args) ->
    ok.


handle_metrics(Request) ->
    Query = elli_request:get_arg_decoded(<<"query">>, Request, <<"*">>),
    case statser_parser:parse(Query) of
        {paths, Path} ->
            lager:debug("handle metrics query: ~p", [Query]),
            Metrics = statser_finder:find_metrics_tree(Path),
            Formatted = format(Metrics, json),
            {ok, ?DEFAULT_HEADERS, Formatted};
        _Invalid ->
            lager:warning("invalid metrics query: ~p", [Query]),
            {400, [], <<"invalid query specified">>}
    end.


handle_render([], _From, _Until, _MaxPoints) ->
    {400, [], <<"no target(s) specified">>};
handle_render(_Targets, false, _Until, _MaxPoints) ->
    {400, [], <<"no 'from' specified">>};
handle_render(_Targets, _From, false, _MaxPoints) ->
    {400, [], <<"no 'until' specified">>};
handle_render(Targets, From, Until, MaxPoints) ->
    Now = erlang:system_time(second),
    Processed = lists:flatmap(fun(Target) ->
                                      % one target definition may result in 0-n results
                                      process_target(Target, From, Until, Now, MaxPoints)
                              end, Targets),
    Formatted = format(lists:map(fun format_to_json/1, Processed), json),
    {ok, ?DEFAULT_HEADERS, Formatted}.


format_to_json(#series{target=Target, values=DataPoints}) ->
    Points = lists:map(fun({TS, Value}) -> [Value, TS] end, DataPoints),
    {[{<<"target">>, Target},
      {<<"datapoints">>, Points}]}.


process_target(Target, From, Until, Now, MaxPoints) ->
    lager:debug("render request for target ~p [~p - ~p] [~w]", [Target, From, Until, MaxPoints]),
    Parsed = statser_parser:parse(Target),
    lager:debug("parsed request target: ~p", [Parsed]),
    Parameters = {From, Until, MaxPoints},
    process(Parsed, Parameters, Now).


process({paths, Path}, Params, Now) -> process_paths(Path, Params, Now);
process({call, Fctn, Args}, Params, Now) -> process_function(Fctn, Args, Params, Now);
process({template, Expr, Args}, Params, Now) -> process_template(Expr, Args, Params, Now);
process(Argument, _Params, _Now) -> Argument.


process_paths(Path, {From, Until, MaxPoints} = Params, Now) ->
    FoundPaths = statser_finder:find_metrics(Path),
    Paths = lists:map(fun({P}) -> proplists:get_value(<<"id">>, P) end, FoundPaths),
    lager:debug("found ~w paths to process: ~p", [length(Paths), Paths]),
    Processed = statser_processor:fetch_data(Paths, From, Until, Now),
    lager:debug("processing resulted in ~p", [Processed]),
    Processed.


process_function(Fctn, Args, {From, Until, MaxPoints} = Params, Now) ->
    ProcessedArgs = lists:map(fun(Arg) -> process(Arg, Params, Now) end, Args),
    Processed = statser_processor:evaluate_call(Fctn, ProcessedArgs, From, Until, Now),
    lager:debug("processing ~p resulted in ~p", [Fctn, Processed]),
    Processed.


process_template(Expr, Args, {From, Until, MaxPoints} = Params, Now) ->
    % TODO: template handling
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


format(Output, _Format) ->
    % XXX: support multiple output formats?
    jiffy:encode(Output).


parse_time(<<"now">>, Now) -> Now;

parse_time(<<"-", Relative/binary>>, Now) ->
    parse_time(Relative, Now, relative);

parse_time(Absolute, Now) ->
    parse_time(Absolute, Now, absolute).


parse_time(Value, _Now, absolute) ->
    List = binary_to_list(Value),
    case string:to_integer(List) of
        {error, _} -> error;
        % this is supposed to be epoch
        {Val, []} -> Val;
        _Otherwise ->
            % TODO: absolute time parsing
            error
    end;

parse_time(Value, Now, relative) ->
    List = binary_to_list(Value),
    case string:to_integer(List) of
        {error, _} -> error;
        {Val, Unit} -> Now - parse_unit(Val, Unit)
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

parse_time_test_() ->
    % easier to test with `now` being 0 instead of real time
    Now = 0,
    [?_assertEqual(error, parse_time(<<"">>, Now)),
     % now
     ?_assertEqual(Now, parse_time(<<"now">>, Now)),
     % relative
     ?_assertEqual(-1, parse_time(<<"-1s">>, Now)),
     ?_assertEqual(-60, parse_time(<<"-1min">>, Now)),
     ?_assertEqual(-365 * 86400, parse_time(<<"-1y">>, Now)),
     % epoch
     ?_assertEqual(1474468456, parse_time(<<"1474468456">>, Now))
    ].

-endif.
