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
    MaxPoints = parse_datapoints(get_or_fallback(<<"maxDataPoints">>, Args, <<"">>)),
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

handle('GET', [<<".statser">> | Path], Req) ->
    statser_dashboard:handle('GET', Path, Req);

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
            statser_instrumentation:increment(<<"api.query.requests">>),
            Metrics = statser_finder_server:find_metrics_tree(Path),
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
    statser_instrumentation:increment(<<"api.render.requests">>),
    Processed = lists:flatmap(fun(Target) ->
                                      % one target definition may result in 0-n results
                                      Res = process_target(Target, From, Until, Now, MaxPoints),
                                      lists:map(fun(S) -> adjust_datapoints(S, MaxPoints) end, Res)
                              end, Targets),
    % sort the processed series in here so the result is stable
    Sorted = lists:sort(fun by_target/2, Processed),
    Formatted = format(lists:map(fun format_to_json/1, Sorted), json),
    {ok, ?DEFAULT_HEADERS, Formatted}.


by_target(#series{target=TargetA}, #series{target=TargetB}) when TargetA =< TargetB -> true;
by_target(_, _) -> false.


adjust_datapoints(Series, none) -> Series;
adjust_datapoints(Series, MaxDataPoints) ->
    Step = Series#series.step,
    TimeRange = Series#series.until - Series#series.start,
    DataPoints = TimeRange div Step,
    case DataPoints > MaxDataPoints of
        true ->
            ValuesPP = statser_util:ceiling(DataPoints / MaxDataPoints),
            % XXX: 'nudge' necessary?
            statser_processor:consolidate(Series, ValuesPP);
        false ->
            Series
    end.


format_to_json(#series{target=Target, values=DataPoints}) ->
    Points = lists:map(fun({TS, Value}) -> [Value, TS] end, DataPoints),
    {[{<<"target">>, Target},
      {<<"datapoints">>, Points}]}.


process_target(Target, From, Until, Now, MaxPoints) ->
    lager:debug("render request for target ~p [~p - ~p] [~w]", [Target, From, Until, MaxPoints]),
    case statser_parser:parse(Target) of
        {_Parsed, _Rest, {{line, _Line},{column, _Col}}} ->
            lager:warning("failed to parse target: ~p", [Target]),
            [];
        Parsed ->
            lager:debug("parsed request target: ~p", [Parsed]),
            Parameters = {From, Until, MaxPoints},
            process(Parsed, Parameters, Now)
    end.


process({paths, Path}, Params, Now) -> process_paths(Path, Params, Now);
process({call, Fctn, Args}, Params, Now) -> process_function(Fctn, Args, Params, Now);
process({template, Expr, Args}, Params, Now) -> process_template(Expr, Args, Params, Now);
process(Argument, _Params, _Now) -> Argument.


process_paths(Path, {From, Until, _MaxPoints}, Now) ->
    Paths = statser_finder_server:find_metrics(Path),
    lager:debug("found ~w paths to process: ~p", [length(Paths), Paths]),
    Processed = statser_processor:fetch_data(Paths, From, Until, Now),
    lager:debug("processing resulted in ~p", [Processed]),
    Processed.


process_function(Fctn, Args, {From, Until, _MaxPoints} = Params, Now) ->
    ProcessedArgs = lists:map(fun(Arg) -> process(Arg, Params, Now) end, Args),
    Processed = statser_processor:evaluate_call(Fctn, ProcessedArgs, From, Until, Now),
    lager:debug("processing ~p resulted in ~p", [Fctn, Processed]),
    Processed.


process_template(_Expr, _Args, {_From, _Until, _MaxPoints} = _Params, _Now) ->
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
    case statser_util:parse_unit(Value) of
        error -> error;
        Val -> Now - Val
    end.


parse_datapoints(Value) ->
    List = binary_to_list(Value),
    case string:to_integer(List) of
        {error, _} -> none;
        {Val, []} -> Val;
        _Otherwise -> none
    end.


%%
%% TESTS
%%

-ifdef(TEST).

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

parse_test_() ->
    [?_assertEqual({call,<<"aliasByMetric">>, [{paths,[<<"foo">>,<<"bar">>]},<<"test">>]},
                   statser_parser:parse(<<"aliasByMetric(foo.bar,'test')">>)),
     ?_assertEqual({call,<<"aliasByMetric">>, [{paths,[<<"foo">>,<<"*">>]},<<"test">>]},
                   statser_parser:parse(<<"aliasByMetric(foo.*,'test')">>)),
     ?_assertEqual({call,<<"aliasByMetric">>, [{paths,[<<"foo">>,<<"{one,two}">>]},<<"test">>]},
                   statser_parser:parse(<<"aliasByMetric(foo.{one,two},'test')">>))
    ].

-endif.
