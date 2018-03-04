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

-module(statser_config).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([load_config/0,
         load_config/1,
         get_metadata/1,
         get_udp_config/0,
         get_data_dir/0,
         get_metric_filters/0,
         get_rate_limits/0,
         get_protobuf_config/0,
         get_tcp_config/0,
         get_api_config/0,
         metric_passes_filters/2,
         udp_is_enabled/1,
         protobuf_is_enabled/1]).


-define(STATSER_DEFAULT_CONFIG, "statser.yaml").

-define(FALLBACK_METRICS_DATA_DIR, <<".">>).

-define(FALLBACK_RATE_LIMITS, #rate_limit_config{creates_per_sec=25,
                                                 updates_per_sec=500}).

-define(FALLBACK_UDP_CONFIG, #udp_config{port=8125,
                                         interval=10000,
                                         prune_after=300000}).

-define(FALLBACK_PROTOBUF_CONFIG, #protobuf_config{port=0}).

-define(FALLBACK_TCP_CONFIG, #tcp_config{port=2003}).

-define(FALLBACK_API_CONFIG, #api_config{port=8080}).

-define(FALLBACK_RETENTIONS, [#retention_definition{raw="1m:1d",
                                                    seconds=60,
                                                    points=1440}]).

-define(FALLBACK_STORAGE, #storage_definition{name="fallback",
                                              retentions=?FALLBACK_RETENTIONS}).

-define(FALLBACK_AGGREGATION, #aggregation_definition{name="fallback",
                                                      aggregation=average,
                                                      factor=0.5}).

-define(FALLBACK_METRIC_FILTERS, #metric_filters{whitelist=[], blacklist=[]}).


-spec load_config() -> ok.
load_config() -> load_config(?STATSER_DEFAULT_CONFIG).


-spec load_config(string()) -> ok.
load_config(ConfigFile) ->
    Docs = try
               % load yaml
               yamerl_constr:file(ConfigFile)
           catch
               Error ->
                   lager:warning("failed to load configuration from ~p - fallback to defaults: ~p", [ConfigFile, Error]),
                   []
           end,

    % parse contents
    {Storages, Aggregations, WL, BL, Udp, Protobuf, Api, Tcp, DataDir, RateLimits} = load_documents(Docs),

    update(storages, Storages),
    update(aggregations, Aggregations),
    update(metric_filters, #metric_filters{whitelist=WL, blacklist=BL}),
    update(udp_config, Udp),
    update(protobuf_config, Protobuf),
    update(api_config, Api),
    update(tcp_config, Tcp),
    update(data_dir, DataDir),
    update(rate_limits, RateLimits).


get_metadata(Path) ->
    Storages = application:get_env(statser, storages, []),
    StorDefinition = first_storage(Path, Storages),

    Aggregations = application:get_env(statser, aggregations, []),
    AggDefinition = first_aggregation(Path, Aggregations),

    {StorDefinition, AggDefinition}.


get_metric_filters() ->
    application:get_env(statser, metric_filters, ?FALLBACK_METRIC_FILTERS).


get_udp_config() ->
    application:get_env(statser, udp_config, ?FALLBACK_UDP_CONFIG).


get_protobuf_config() ->
    application:get_env(statser, protobuf_config, ?FALLBACK_PROTOBUF_CONFIG).


get_api_config() ->
    application:get_env(statser, api_config, ?FALLBACK_API_CONFIG).


get_tcp_config() ->
    application:get_env(statser, tcp_config, ?FALLBACK_TCP_CONFIG).


get_data_dir() ->
    application:get_env(statser, data_dir, ?FALLBACK_METRICS_DATA_DIR).


get_rate_limits() ->
    application:get_env(statser, rate_limits, ?FALLBACK_RATE_LIMITS).


metric_passes_filters(Metric, #metric_filters{whitelist=WhiteList, blacklist=BlackList}) ->
    passes_blacklist(Metric, BlackList) andalso passes_whitelist(Metric, WhiteList);

metric_passes_filters(_Metric, _InvalidOrNoFilters) ->
    true.


%%%===================================================================
%%% Internal functions
%%%===================================================================

passes_whitelist(_Metric, []) -> true;
passes_whitelist(Metric, WL) ->
    passes_whitelist_inner(Metric, WL).


passes_whitelist_inner(_Metric, []) -> false;
passes_whitelist_inner(Metric, [#metric_pattern{pattern=Pattern} | Ps]) ->
    case re:run(Metric, Pattern) of
        {match, _} -> true;
        _Otherwise -> passes_whitelist_inner(Metric, Ps)
    end.


passes_blacklist(_Metric, []) -> true;
passes_blacklist(Metric, [#metric_pattern{pattern=Pattern} | Ps]) ->
    case re:run(Metric, Pattern) of
        {match, _} -> false;
        _Otherwise -> passes_blacklist(Metric, Ps)
    end.


first_or_fallback(_GetPattern, Fallback, _Path, []) -> Fallback;
first_or_fallback(GetPattern, Fallback, Path, [C | Cs]) ->
    Pattern = GetPattern(C),
    case re:run(Path, Pattern) of
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

update(_Type, _Values) -> ok.

-else.

update(Type, Values) ->
    % store into application environment
    application:set_env(statser, Type, Values),
    ok.

-endif. % TEST


load_documents([Document]) ->
    load_document(Document);

load_documents([]) ->
    {[], [], [], [],
     ?FALLBACK_UDP_CONFIG,
     ?FALLBACK_PROTOBUF_CONFIG,
     ?FALLBACK_API_CONFIG,
     ?FALLBACK_TCP_CONFIG,
     ?FALLBACK_METRICS_DATA_DIR,
     ?FALLBACK_RATE_LIMITS
    }.


load_document(Doc) ->
    Storages = load_storage(proplists:get_value("storage", Doc, [])),
    lager:info("loaded ~w storage definitions", [length(Storages)]),

    Aggregations = load_aggregation(proplists:get_value("aggregation", Doc, [])),
    lager:info("loaded ~w aggregation definitions", [length(Aggregations)]),

    GetFilter = fun(#metric_pattern{name=Name}) -> Name end,

    WhiteList = load_list_expressions(proplists:get_value("whitelist", Doc)),
    lager:info("loaded whitelist: ~p", [lists:map(GetFilter, WhiteList)]),

    BlackList = load_list_expressions(proplists:get_value("blacklist", Doc)),
    lager:info("loaded blacklist: ~p", [lists:map(GetFilter, BlackList)]),

    Udp = load_udp_config(proplists:get_value("udp", Doc, [])),
    lager:info("loaded UDP config: ~p", [Udp]),

    Protobuf = load_protobuf_config(proplists:get_value("protobuf", Doc, [])),
    lager:info("loaded protobuf config: ~p", [Protobuf]),

    Api = load_api_config(proplists:get_value("api", Doc, [])),
    lager:info("loaded API config: ~p", [Api]),

    Tcp = load_tcp_config(proplists:get_value("tcp", Doc, [])),
    lager:info("loaded TCP listener config: ~p", [Tcp]),

    DataDir = load_data_dir(proplists:get_value("data_dir", Doc)),
    lager:info("loaded data directory config: ~s", [DataDir]),

    RateLimits = load_rate_limits(proplists:get_value("rate_limits", Doc)),
    lager:info("loaded rate limits: ~p", [RateLimits]),

    {Storages, Aggregations, WhiteList, BlackList, Udp, Protobuf, Api, Tcp, DataDir, RateLimits}.


load_data_dir(Value) when is_list(Value) ->
    case lists:all(fun erlang:is_integer/1, Value) of
        true -> list_to_binary(Value);
        false -> ?FALLBACK_METRICS_DATA_DIR
    end;
load_data_dir(_) -> ?FALLBACK_METRICS_DATA_DIR.


load_rate_limits(Xs) ->
    load_rate_limits(Xs, ?FALLBACK_RATE_LIMITS).

load_rate_limits([{"creates", PerSec} | Xs], Limits) when is_number(PerSec) ->
    load_rate_limits(Xs, Limits#rate_limit_config{creates_per_sec=PerSec});

load_rate_limits([{"updates", PerSec} | Xs], Limits) when is_number(PerSec) ->
    load_rate_limits(Xs, Limits#rate_limit_config{updates_per_sec=PerSec});

load_rate_limits([_ | Xs], Limits) ->
    load_rate_limits(Xs, Limits);

load_rate_limits(_Invalid, Limits) ->
    Limits.


load_udp_config(Xs) ->
    load_udp_config(Xs, ?FALLBACK_UDP_CONFIG).

load_udp_config([{"port", Port} | Xs], Config) when is_number(Port) ->
    load_udp_config(Xs, Config#udp_config{port=Port});

load_udp_config([{"interval", Interval} | Xs], Config) when is_number(Interval) ->
    % config value is expected to be in seconds
    load_udp_config(Xs, Config#udp_config{interval=Interval * ?MILLIS_PER_SEC});

load_udp_config([{"prune_after", Interval} | Xs], Config) when is_number(Interval) ->
    % config value is expected to be in seconds
    load_udp_config(Xs, Config#udp_config{prune_after=Interval * ?MILLIS_PER_SEC});

load_udp_config([_ | Xs], Config) ->
    load_udp_config(Xs, Config);

load_udp_config(_Invalid, Config) -> Config.


udp_is_enabled(#udp_config{port=Port}) when is_number(Port) ->
    Port > 0;
udp_is_enabled(_) -> false.


load_protobuf_config(Xs) ->
    load_protobuf_config(Xs, ?FALLBACK_PROTOBUF_CONFIG).

load_protobuf_config([{"port", Port} | Xs], Config) when is_number(Port) ->
    load_protobuf_config(Xs, Config#protobuf_config{port=Port});
load_protobuf_config([_ | Xs], Config) ->
    load_protobuf_config(Xs, Config);
load_protobuf_config(_Invalid, Config) ->
    Config.


load_api_config(Xs) ->
    load_api_config(Xs, ?FALLBACK_API_CONFIG).

load_api_config([{"port", Port} | Xs], Config) when is_number(Port) ->
    load_api_config(Xs, Config#api_config{port=Port});
load_api_config([_ | Xs], Config) ->
    load_api_config(Xs, Config);
load_api_config(_Invalid, Config) ->
    Config.


load_tcp_config(Xs) ->
    load_tcp_config(Xs, ?FALLBACK_TCP_CONFIG).

load_tcp_config([{"port", Port} | Xs], Config) when is_number(Port) ->
    load_tcp_config(Xs, Config#tcp_config{port=Port});
load_tcp_config([_ | Xs], Config) ->
    load_tcp_config(Xs, Config);
load_tcp_config(_Invalid, Config) ->
    Config.


protobuf_is_enabled(#protobuf_config{port=Port}) when is_number(Port) ->
    Port > 0;
protobuf_is_enabled(_Other) -> false.


load_list_expressions(Ls) ->
    load_list_expressions(Ls, []).

load_list_expressions([], Acc) -> lists:reverse(Acc);
load_list_expressions([Expr | Rst], Acc) when is_list(Expr) ->
    lager:debug("found regex pattern: ~p", [Expr]),

    case re:compile(Expr, [no_auto_capture]) of
        {ok, Regex} ->
            New = #metric_pattern{pattern=Regex, name=Expr},
            load_list_expressions(Rst, [New | Acc]);
        {error, Err} ->
            lager:warning("skipping invalid metric pattern '~w': ~p", [Expr, Err]),
            load_list_expressions(Rst, Acc)
    end;

load_list_expressions([_Expr | Rst], Acc) ->
    load_list_expressions(Rst, Acc);

load_list_expressions(_Other, Acc) -> Acc.


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
    GetAggregation = fun({_Storage, Aggs, _WL, _BL, _Udp, _PB, _Api, _Tcp, _Dir, _RL}) -> Aggs end,
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


load_whitelist_from_string_test_() ->
    GetWL = fun({_Storage, _Aggs, WL, _BL, _Udp, _PB, _Api, _Tcp, _Dir, _RL}) -> WL end,
    {setup, fun setup/0,
     [?_assertEqual([], load_from_string("", GetWL)),
      ?_assertEqual([], load_from_string("whitelist:", GetWL)),
      ?_assertEqual([], load_from_string("whitelist: invalid", GetWL)),
      ?_assertEqual([], load_from_string("whitelist:\n - inv**alid", GetWL)),
      ?_assertEqual(1, length(load_from_string("whitelist:\n - ^stats\.", GetWL))),
      ?_assertEqual(2, length(load_from_string("whitelist:\n - ^stats\.\n - \.mean$", GetWL)))
    ]}.


load_blacklist_from_string_test_() ->
    GetBL = fun({_Storage, _Aggs, _WL, BL, _Udp, _PB, _Api, _Tcp, _Dir, _RL}) -> BL end,
    {setup, fun setup/0,
     [?_assertEqual([], load_from_string("", GetBL)),
      ?_assertEqual([], load_from_string("blacklist:", GetBL)),
      ?_assertEqual([], load_from_string("blacklist: invalid", GetBL)),
      ?_assertEqual([], load_from_string("blacklist:\n - inv**alid", GetBL)),
      ?_assertEqual(1, length(load_from_string("blacklist:\n - ^stats\.", GetBL))),
      ?_assertEqual(2, length(load_from_string("blacklist:\n - ^stats\.\n - \.mean$", GetBL)))
    ]}.


load_rate_limits_from_string_test_() ->
    GetLimits = fun({_Storage, _Aggs, _WL, _BL, _Udp, _PB, _Api, _Tcp, _Dir, RL}) -> RL end,
    {setup, fun setup/0,
     [?_assertEqual(?FALLBACK_RATE_LIMITS, load_from_string("", GetLimits)),
      ?_assertEqual(?FALLBACK_RATE_LIMITS, load_from_string("rate_limits:", GetLimits)),
      ?_assertEqual(?FALLBACK_RATE_LIMITS, load_from_string("rate_limits: invalid", GetLimits)),
      ?_assertEqual(#rate_limit_config{creates_per_sec=13, updates_per_sec=202},
                    load_from_string("rate_limits:\n creates: 13\n updates: 202", GetLimits))
     ]}.


load_udp_config_from_string_test_() ->
    GetUdp = fun({_Storage, _Aggs, _WL, _BL, Udp, _PB, _Api, _Tcp, _Dir, _RL}) -> Udp end,
    {setup, fun setup/0,
     [?_assertEqual(?FALLBACK_UDP_CONFIG, load_from_string("", GetUdp)),
      ?_assertEqual(?FALLBACK_UDP_CONFIG, load_from_string("udp:", GetUdp)),
      ?_assertEqual(?FALLBACK_UDP_CONFIG, load_from_string("udp: invalid", GetUdp)),
      ?_assertEqual(#udp_config{port=8000, interval=15000, prune_after=60000},
                    load_from_string("udp:\n port: 8000\n interval: 15\n prune_after: 60", GetUdp))
     ]}.


load_protobuf_config_from_string_test_() ->
    GetPB = fun({_Storage, _Aggs, _WL, _BL, _Udp, PB, _Api, _Tcp, _Dir, _RL}) -> PB end,
    {setup, fun setup/0,
     [?_assertEqual(?FALLBACK_PROTOBUF_CONFIG, load_from_string("", GetPB)),
      ?_assertEqual(?FALLBACK_PROTOBUF_CONFIG, load_from_string("protobuf:", GetPB)),
      ?_assertEqual(?FALLBACK_PROTOBUF_CONFIG, load_from_string("protobuf: invalid", GetPB)),
      ?_assertEqual(#protobuf_config{port=10010},
                    load_from_string("protobuf:\n port: 10010", GetPB))
     ]}.


load_api_config_from_string_test_() ->
    GetApi = fun({_Storage, _Aggs, _WL, _BL, _Udp, _PB, Api, _Tcp, _Dir, _RL}) -> Api end,
    {setup, fun setup/0,
     [?_assertEqual(?FALLBACK_API_CONFIG, load_from_string("", GetApi)),
      ?_assertEqual(?FALLBACK_API_CONFIG, load_from_string("api:", GetApi)),
      ?_assertEqual(?FALLBACK_API_CONFIG, load_from_string("api: invalid", GetApi)),
      ?_assertEqual(#api_config{port=10010},
                    load_from_string("api:\n port: 10010", GetApi))
     ]}.


load_tcp_config_from_string_test_() ->
    GetTcp = fun({_Storage, _Aggs, _WL, _BL, _Udp, _PB, _Api, Tcp, _Dir, _RL}) -> Tcp end,
    {setup, fun setup/0,
     [?_assertEqual(?FALLBACK_TCP_CONFIG, load_from_string("", GetTcp)),
      ?_assertEqual(?FALLBACK_TCP_CONFIG, load_from_string("tcp:", GetTcp)),
      ?_assertEqual(?FALLBACK_TCP_CONFIG, load_from_string("tcp: invalid", GetTcp)),
      ?_assertEqual(#tcp_config{port=10010},
                    load_from_string("tcp:\n port: 10010", GetTcp))
     ]}.


load_data_dir_from_string_test_() ->
    GetDir = fun({_Storage, _Aggs, _WL, _BL, _Udp, _PB, _Api, _Tcp, Dir, _RL}) -> Dir end,
    {setup, fun setup/0,
     [?_assertEqual(?FALLBACK_METRICS_DATA_DIR, load_from_string("", GetDir)),
      ?_assertEqual(?FALLBACK_METRICS_DATA_DIR, load_from_string("data_dir:", GetDir)),
      ?_assertEqual(?FALLBACK_METRICS_DATA_DIR, load_from_string("data_dir: 3252", GetDir)),
      ?_assertEqual(<<"/tmp/metrics">>,
                    load_from_string("data_dir: /tmp/metrics", GetDir))
     ]}.


udp_is_enabled_test_() ->
    GetUdp = fun({_Storage, _Aggs, _WL, _BL, Udp, _PB, _Api, _Tcp, _Dir, _RL}) -> Udp end,
    {setup, fun setup/0,
     [?_assertEqual(true, udp_is_enabled(?FALLBACK_UDP_CONFIG)),
      ?_assertEqual(true, udp_is_enabled(load_from_string("udp:\n port: 8000", GetUdp))),
      ?_assertEqual(false, udp_is_enabled(load_from_string("udp:\n port: 0", GetUdp)))
     ]}.


load_storage_from_string_test_() ->
    GetStorage = fun({Storage, _Aggs, _WL, _BL, _Udp, _PB, _Api, _Tcp, _Dir, _RL}) -> Storage end,
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
      ?_assertEqual(ok, load_config("test/examples/does_not_exist.yaml"))
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


to_filters(WhiteList, BlackList) ->
    ToPattern = fun(Pattern) ->
                        {ok, Pat} = re:compile(Pattern, [no_auto_capture]),
                        #metric_pattern{pattern=Pat, name=Pattern}
                end,
    #metric_filters{whitelist=lists:map(ToPattern, WhiteList),
                    blacklist=lists:map(ToPattern, BlackList)}.


metric_passes_filters_test_() ->
    [?_assertEqual(true, metric_passes_filters(<<"foo.bar">>, to_filters([], []))),
     ?_assertEqual(true, metric_passes_filters(<<"foo.bar">>, to_filters(["^foo\."], []))),
     ?_assertEqual(true, metric_passes_filters(<<"foo.bar">>, to_filters(["\.bar$"], []))),
     ?_assertEqual(true, metric_passes_filters(<<"foo.bar">>, to_filters(["ba"], []))),
     ?_assertEqual(true, metric_passes_filters(<<"foo.bar">>, to_filters(["^bar", "bar"], []))),
     ?_assertEqual(true, metric_passes_filters(<<"foo.bar">>, to_filters(["^foo"], ["^stats"]))),
     ?_assertEqual(false, metric_passes_filters(<<"foo.bar">>, to_filters(["^bar\."], []))),
     ?_assertEqual(false, metric_passes_filters(<<"foo.bar">>, to_filters(["^foo"], ["bar$"]))),
     ?_assertEqual(false, metric_passes_filters(<<"foo.bar">>, to_filters([], ["bar$"]))),
     ?_assertEqual(true, metric_passes_filters(<<"foo.bar">>, to_filters([], ["foo$"]))),
     ?_assertEqual(false, metric_passes_filters(<<"foo.bar">>, to_filters([], ["foo$", "foo"])))
    ].

-endif. % TEST
