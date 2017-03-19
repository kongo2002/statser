-module(statser_config).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([load_config/0,
         load_config/1,
         get_metadata/1]).


-define(STATSER_DEFAULT_CONFIG, "statser.yaml").

-define(FALLBACK_RETENTIONS, [#retention_definition{raw="1m:1d",
                                                    seconds=60,
                                                    points=1440}]).

-define(FALLBACK_STORAGE, #storage_definition{retentions=?FALLBACK_RETENTIONS}).

-define(FALLBACK_AGGREGATION, #aggregation_definition{aggregation=average,
                                                      factor=0.5}).


-spec load_config() -> ok | error.
load_config() -> load_config(?STATSER_DEFAULT_CONFIG).


-spec load_config(string()) -> ok | error.
load_config(ConfigFile) ->
    try
        % load yaml
        Docs = yamerl_constr:file(ConfigFile),

        % parse contents
        {Storages, Aggregations} = load_documents(Docs),
        update(Storages, Aggregations)
    catch
        _ -> error
    end.


get_metadata(Path) ->
    Storages = application:get_env(statser, storages, []),
    StorDefinition = first_storage(Path, Storages),

    Aggregations = application:get_env(statser, aggregations, []),
    AggDefinition = first_aggregation(Path, Aggregations),

    Retentions = lists:map(fun (#retention_definition{seconds=S, points=P}) -> {S, P} end,
                           StorDefinition#storage_definition.retentions),

    Aggregation = AggDefinition#aggregation_definition.aggregation,
    XFF = AggDefinition#aggregation_definition.factor,

    % not sure if we should return the whole 'definition' structures instead
    {Retentions, Aggregation, XFF}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

first_or_fallback(_GetPattern, Fallback, _Path, []) -> Fallback;
first_or_fallback(GetPattern, Fallback, Path, [C | Cs]) ->
    Pattern = GetPattern(C),
    case re:run(Path, Pattern) of
        match -> C;
        {match, _} -> C;
        _Otherwise -> first_or_fallback(GetPattern, Fallback, Path, Cs)
    end.


first_storage(Path, Candidates) ->
    GetPattern = fun (S) -> S#storage_definition.pattern end,
    first_or_fallback(GetPattern, ?FALLBACK_STORAGE, Path, Candidates).


first_aggregation(Path, Candidates) ->
    GetPattern = fun (S) -> S#aggregation_definition.pattern end,
    first_or_fallback(GetPattern, ?FALLBACK_AGGREGATION, Path, Candidates).


-ifdef(TEST).

update(_Storages, _Aggregations) -> ok.

-else.

update(Storages, Aggregations) ->
    % store into application environment
    application:set_env(statser, storages, Storages),
    application:set_env(statser, aggregations, Aggregations),
    ok.

-endif. % TEST


load_documents([Document]) -> load_document(Document);
load_documents([]) -> {[], []}.


load_document(Doc) ->
    Storages = load_storage(proplists:get_value("storage", Doc, [])),
    lager:info("loaded ~w storage definitions", [length(Storages)]),

    Aggregations = load_aggregation(proplists:get_value("aggregation", Doc, [])),
    lager:info("loaded ~w aggregation definitions", [length(Aggregations)]),

    {Storages, Aggregations}.


load_storage(Ss) -> load_storage(Ss, []).

load_storage([], Acc) -> lists:reverse(Acc);
load_storage([{Name, Elements} = Storage | Ss], Acc) ->
    lager:debug("found storage definition: ~p", [Storage]),

    Pattern = load_mapping("pattern", Elements, undefined),
    Retentions = load_mapping("retentions", Elements, []),
    ParsedRetentions = parse_retentions(Retentions),

    case {Pattern, ParsedRetentions} of
        {undefined, _} ->
            lager:warning("skipping invalid storage definition ~p", [Name]),
            load_storage(Ss, Acc);
        {_, []} ->
            lager:warning("skipping invalid storage definition ~p", [Name]),
            load_storage(Ss, Acc);
        {P, Rs} ->
            case re:compile(P, [no_auto_capture]) of
                {ok, Regex} ->
                    New = #storage_definition{name=Name, pattern=Regex, retentions=Rs},
                    load_storage(Ss, [New | Acc]);
                {error, Err} ->
                    lager:warning("skipping invalid storage definition '~w': ~p", [Name, Err]),
                    load_storage(Ss, Acc)
            end
    end;
load_storage(_Invalid, Acc) -> Acc.


parse_retentions(Rs) when is_list(Rs) -> parse_retentions(Rs, []);
parse_retentions(_) -> [].

parse_retentions([], Acc) -> Acc;
parse_retentions([RawRetention | Rs], Acc) when is_list(RawRetention) ->
    case string:tokens(RawRetention, ":") of
        [SecStr, PointStr] ->
            Duration = parse_duration(SecStr),
            Retention = parse_duration(PointStr),

            case {Duration, Retention} of
                % here the retention is specified as 'number of data points'
                {{ok, D, DU}, {ok, R, default}} ->
                    Definition = #retention_definition{raw=RawRetention, seconds=unit_to_seconds(D, DU), points=R},
                    parse_retentions(Rs, [Definition | Acc]);
                % whereas in here it's given in a duration format (i.e. '7d')
                % that's why we have to calculate towards the number of data points
                {{ok, D, DU}, {ok, R, RU}} ->
                    Precision = unit_to_seconds(D, DU),
                    RetPoints = unit_to_seconds(R, RU) div Precision,
                    Definition = #retention_definition{raw=RawRetention, seconds=Precision, points=RetPoints},
                    parse_retentions(Rs, [Definition | Acc]);
                _Otherwise ->
                    lager:warning("skipping invalid retention definition: ~p", [RawRetention]),
                    parse_retentions(Rs, Acc)
            end;
        Invalid ->
            lager:warning("invalid retention string found ~p - expecting format '<precision>:<duration>'", [Invalid]),
            parse_retentions(Rs, Acc)
    end;
parse_retentions([_RawRetention | Rs], Acc) ->
    lager:warning("retentions are expected to be a list"),
    parse_retentions(Rs, Acc).


parse_duration(Str) ->
    case string:to_integer(Str) of
        {error, _Reason} -> error;
        {Value, Unit} when Value > 0 -> parse_unit(Value, Unit);
        _Otherwise -> error
    end.


parse_unit(Value, []) -> {ok, Value, default};
parse_unit(Value, [$s | _]) -> {ok, Value, seconds};
parse_unit(Value, [$S | _]) -> {ok, Value, seconds};
parse_unit(Value, [$m | _]) -> {ok, Value, minutes};
parse_unit(Value, [$M | _]) -> {ok, Value, minutes};
parse_unit(Value, [$h | _]) -> {ok, Value, hours};
parse_unit(Value, [$H | _]) -> {ok, Value, hours};
parse_unit(Value, [$d | _]) -> {ok, Value, days};
parse_unit(Value, [$D | _]) -> {ok, Value, days };
parse_unit(Value, [$w | _]) -> {ok, Value, weeks};
parse_unit(Value, [$W | _]) -> {ok, Value, weeks};
parse_unit(Value, [$y | _]) -> {ok, Value, years};
parse_unit(Value, [$Y | _]) -> {ok, Value, years};
parse_unit(_, _) -> error.


-spec unit_to_seconds(integer(), duration_unit()) -> integer().
unit_to_seconds(Value, default) -> Value;
unit_to_seconds(Value, seconds) -> Value;
unit_to_seconds(Value, minutes) -> Value * 60;
unit_to_seconds(Value, hours) -> Value * 3600;
unit_to_seconds(Value, days) -> Value * 86400;
unit_to_seconds(Value, weeks) -> Value * 86400 * 7;
unit_to_seconds(Value, years) -> Value * 86400 * 365.


load_aggregation(As) -> load_aggregation(As, []).

load_aggregation([], Acc) -> lists:reverse(Acc);
load_aggregation([{Name, Elements} = Aggregation| As], Acc) ->
    lager:debug("found aggregation definition: ~p", [Aggregation]),

    Pattern = load_mapping("pattern", Elements, undefined),
    Agg = load_mapping("aggregation", Elements, []),
    Factor = load_mapping("factor", Elements, 0.5),

    case Pattern of
        undefined ->
            lager:warning("skipping invalid aggregation definition '~w'", [Name]),
            load_aggregation(As, Acc);
        P ->
            case re:compile(P, [no_auto_capture]) of
                {ok, Regex} ->
                    ParsedAgg = parse_aggregation(Agg),
                    New = #aggregation_definition{name=Name,
                                                  pattern=Regex,
                                                  aggregation=ParsedAgg,
                                                  factor=Factor},
                    load_aggregation(As, [New | Acc]);
                {error, Err} ->
                    lager:warning("skipping invalid aggregation definition '~w': ~p", [Name, Err]),
                    load_aggregation(As, Acc)
            end
    end;
load_aggregation(_Invalid, Acc) -> Acc.


load_mapping(Key, Content, Default) when is_list(Content) ->
    proplists:get_value(Key, Content, Default);
load_mapping(_Key, _Content, Default) -> Default.


-spec parse_aggregation(string()) -> aggregation().
parse_aggregation("average") -> average;
parse_aggregation("sum") -> sum;
parse_aggregation("last") -> last;
parse_aggregation("max") -> max;
parse_aggregation("min") -> min;
parse_aggregation("average_zero") -> average_zero;
parse_aggregation(Unknown) ->
    lager:warning("found unknown aggregate '~p' - falling back to average", [Unknown]),
    average.


%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

setup() ->
    application:start(yamerl).


load_from_string(Str, Func) ->
    Docs = yamerl_constr:string(Str),
    Func(load_documents(Docs)).


load_aggregation_from_string_test_() ->
    GetAggregation = fun({_Storage, Aggs}) -> Aggs end,
    {setup, fun setup/0,
     [% empty or stub configurations
      ?_assertEqual([], load_from_string("", GetAggregation)),
      ?_assertEqual([], load_from_string("storage:", GetAggregation)),
      ?_assertEqual([], load_from_string("aggregation:", GetAggregation)),
      ?_assertEqual([], load_from_string("foo:", GetAggregation)),
      ?_assertEqual([], load_from_string("storage: invalid", GetAggregation)),
      ?_assertEqual([], load_from_string("storage: 0", GetAggregation)),
      % simple aggregation definition(s)
      ?_assertEqual(1, length(load_from_string(
                                "aggregation:\n test:\n  pattern: sum$\n  aggregation: sum", GetAggregation)))
     ]}.


load_aggregate_from_file_test_() ->
    {setup, fun setup/0,
     [?_assertEqual(ok, load_config("test/examples/aggregation1.yaml")),
      ?_assertEqual(ok, load_config("test/examples/aggregation2.yaml")),
      ?_assertEqual(ok, load_config("test/examples/aggregation3.yaml"))
     ]}.


load_storage_from_string_test_() ->
    GetStorage = fun({Storage, _Aggs}) -> Storage end,
    {setup, fun setup/0,
     [% empty or stub configurations
      ?_assertEqual([], load_from_string("", GetStorage)),
      ?_assertEqual([], load_from_string("storage:", GetStorage)),
      ?_assertEqual([], load_from_string("aggregation:", GetStorage)),
      ?_assertEqual([], load_from_string("foo:", GetStorage)),
      ?_assertEqual([], load_from_string("storage: invalid", GetStorage)),
      ?_assertEqual([], load_from_string("storage: 0", GetStorage)),
      % simple storage definition(s)
      ?_assertEqual(1, length(load_from_string(
                                "storage:\n test:\n  pattern: ^stats\n  retentions: ['10:60']", GetStorage)))
     ]}.


load_storage_from_file_test_() ->
    {setup, fun setup/0,
     [?_assertEqual(ok, load_config("test/examples/storage1.yaml")),
      ?_assertEqual(ok, load_config("test/examples/storage2.yaml")),
      ?_assertEqual(ok, load_config("test/examples/storage3.yaml")),
      ?_assertEqual(error, load_config("test/examples/does_not_exist.yaml"))
     ]}.


parse_duration_test_() ->
    [?_assertEqual({ok, 100, default}, parse_duration("100")),
     ?_assertEqual({ok, 60, seconds}, parse_duration("60s")),
     ?_assertEqual({ok, 1, minutes}, parse_duration("1m")),
     ?_assertEqual({ok, 3, minutes}, parse_duration("3M")),
     ?_assertEqual({ok, 2, hours}, parse_duration("2h")),
     ?_assertEqual({ok, 1, hours}, parse_duration("1H")),
     ?_assertEqual({ok, 1, days}, parse_duration("1d")),
     ?_assertEqual({ok, 4, days}, parse_duration("4D")),
     ?_assertEqual({ok, 2, minutes}, parse_duration("2m ")),
     ?_assertEqual(error, parse_duration("")),
     ?_assertEqual(error, parse_duration("foo")),
     ?_assertEqual(error, parse_duration("0")),
     ?_assertEqual(error, parse_duration("2345.4")),
     ?_assertEqual(error, parse_duration("-3420")),
     ?_assertEqual(error, parse_duration("-1d"))
    ].


parse_retentions_test_() ->
    [?_assertEqual([#retention_definition{
                       raw="10:600",
                       seconds=10,
                       points=600
                      }], parse_retentions(["10:600"])),
     ?_assertEqual([#retention_definition{
                       raw="1m:1w",
                       seconds=60,
                       points=10080
                      }], parse_retentions(["1m:1w"])),
     ?_assertEqual([#retention_definition{
                       raw="60:7D",
                       seconds=60,
                       points=10080
                      }], parse_retentions(["60:7D"])),
     ?_assertEqual([#retention_definition{
                       raw="1h:1y",
                       seconds=3600,
                       points=8760
                      }], parse_retentions(["1h:1y"]))
    ].

-endif. % TEST
