-module(statser_config).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([load_config/0,
         load_config/1]).

-define(STATSER_DEFAULT_CONFIG, "statser.yaml").


-spec load_config() -> ok | error.
load_config() -> load_config(?STATSER_DEFAULT_CONFIG).


-spec load_config(string()) -> ok | error.
load_config(ConfigFile) ->
    try
        Docs = yamerl_constr:file(ConfigFile),
        load_documents(Docs)
    catch
        _ -> error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


load_documents([Document]) -> load_document(Document);
load_documents([]) -> ok.


load_document(Doc) ->
    Storages = load_storage(proplists:get_value("storage", Doc, [])),
    lager:info("loaded ~w storage definitions", [length(Storages)]),

    Aggregations = load_aggregation(proplists:get_value("aggregation", Doc, [])),
    lager:info("loaded ~w aggregation definitions", [length(Aggregations)]),
    ok.


load_storage(Ss) -> load_storage(Ss, []).

load_storage([], Acc) -> lists:reverse(Acc);
load_storage([{Name, Elements} = Storage | Ss], Acc) ->
    lager:debug("found storage definition: ~p", [Storage]),

    Pattern = load_mapping("pattern", Elements, undefined),
    Retentions = load_mapping("retentions", Elements, []),

    case {Pattern, Retentions} of
        {undefined, _} ->
            lager:warning("skipping invalid storage definition '~w'", [Name]),
            load_storage(Ss, Acc);
        {_, []} ->
            lager:warning("skipping invalid storage definition '~w'", [Name]),
            load_storage(Ss, Acc);
        {P, Rs} ->
            case re:compile(P, [no_auto_capture]) of
                {ok, Regex} ->
                    New = #storage_definition{name=Name, pattern=Regex, retentions=[]},
                    load_storage(Ss, [New | Acc]);
                {error, Err} ->
                    lager:warning("skipping invalid storage definition '~w': ~p", [Name, Err]),
                    load_storage(Ss, Acc)
            end
    end.


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
    end.


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
    case yamerl_constr:string(Str) of
        [] -> [];
        [Doc] -> Func(Doc)
    end.


load_aggregation_from_string_test_() ->
    {setup, fun setup/0,
     [% empty or stub configurations
      ?_assertEqual([], load_from_string("", fun load_aggregation/1)),
      ?_assertEqual([], load_from_string("storage:", fun load_aggregation/1)),
      ?_assertEqual([], load_from_string("aggregation:", fun load_aggregation/1)),
      ?_assertEqual([], load_from_string("foo:", fun load_aggregation/1)),
      ?_assertEqual([], load_from_string("storage: invalid", fun load_aggregation/1)),
      ?_assertEqual([], load_from_string("storage: 0", fun load_aggregation/1))
     ]}.


load_aggregate_from_file_test_() ->
    {setup, fun setup/0,
     [?_assertEqual(ok, load_config("test/examples/aggregation1.yaml")),
      ?_assertEqual(ok, load_config("test/examples/aggregation2.yaml")),
      ?_assertEqual(ok, load_config("test/examples/aggregation3.yaml"))
     ]}.


load_storage_from_string_test_() ->
    {setup, fun setup/0,
     [% empty or stub configurations
      ?_assertEqual([], load_from_string("", fun load_storage/1)),
      ?_assertEqual([], load_from_string("storage:", fun load_storage/1)),
      ?_assertEqual([], load_from_string("aggregation:", fun load_storage/1)),
      ?_assertEqual([], load_from_string("foo:", fun load_storage/1)),
      ?_assertEqual([], load_from_string("storage: invalid", fun load_storage/1)),
      ?_assertEqual([], load_from_string("storage: 0", fun load_storage/1))
     ]}.


load_storage_from_file_test_() ->
    {setup, fun setup/0,
     [?_assertEqual(ok, load_config("test/examples/storage1.yaml")),
      ?_assertEqual(ok, load_config("test/examples/storage2.yaml")),
      ?_assertEqual(ok, load_config("test/examples/storage3.yaml")),
      ?_assertEqual(error, load_config("test/examples/does_not_exist.yaml"))
     ]}.

-endif. % TEST
