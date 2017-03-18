-module(statser_config).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([load_config/0]).

-define(STATSER_DEFAULT_CONFIG, "statser.yaml").


-spec load_config() -> ok | error.
load_config() ->
    Docs = yamerl_constr:file(?STATSER_DEFAULT_CONFIG),
    load_documents(Docs).


%%%===================================================================
%%% Internal functions
%%%===================================================================


load_documents([Document]) -> load_document(Document);
load_documents(_) -> error.


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

    Pattern = proplists:get_value("pattern", Elements, undefined),
    Retentions = proplists:get_value("retentions", Elements, []),

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

    Pattern = proplists:get_value("pattern", Elements, undefined),
    Agg = proplists:get_value("aggregation", Elements, []),
    Factor = proplists:get_value("factor", Elements, 0.5),

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

-endif. % TEST
