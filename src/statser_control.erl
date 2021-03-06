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

-module(statser_control).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("api.hrl").
-include("statser.hrl").

-export([handle/3]).


%%%===================================================================
%%% API
%%%===================================================================

%---------------------------------------------------------------------
% NODES API
%---------------------------------------------------------------------

handle('GET', [<<"nodes">>], _Req) ->
    Nodes = statser_discoverer:get_nodes(),
    Nodes0 = lists:map(fun node_to_json/1, Nodes),
    json(Nodes0);

handle('DELETE', [<<"nodes">>], Req) ->
    Body = elli_request:body(Req),
    lager:debug("DELETE /control/nodes: ~p", [Body]),

    case parse_json_of(Body, fun parse_node_info/1) of
        {ok, Node} ->
            case statser_discoverer:disconnect(Node) of
                true -> ok();
                _Else -> bad_request(<<"disconnect failed">>)
            end;
        {error, Error} ->
            bad_request(Error)
    end;

handle('POST', [<<"nodes">>], Req) ->
    Body = elli_request:body(Req),
    lager:debug("POST /control/nodes: ~p", [Body]),

    case parse_json_of(Body, fun parse_node_info/1) of
        {ok, Node} ->
            case statser_discoverer:connect(Node) of
                true -> ok();
                _Else -> bad_request(<<"failed to connect">>)
            end;
        {error, Error} ->
            bad_request(Error)
    end;

%---------------------------------------------------------------------
% STORAGE API
%---------------------------------------------------------------------

handle('GET', [<<"storages">>], _Req) ->
    Storages = statser_config:get_storages(),
    json(lists:map(fun prepare_storage/1, Storages));

handle('POST', [<<"storages">>], Req) ->
    Body = elli_request:body(Req),
    lager:debug("POST /control/storages: ~p", [Body]),

    case parse_json_of(Body, fun storage_from_json/1) of
        #storage_definition{}=Storage ->
            statser_config:add_storage(Storage),
            ok();
        {error, Error} ->
            bad_request(Error);
        error ->
            bad_request(<<"invalid storage definition given">>)
    end;

%---------------------------------------------------------------------
% AGGREGATION API
%---------------------------------------------------------------------

handle('GET', [<<"aggregations">>], _Req) ->
    Aggregations = statser_config:get_aggregations(),
    json(lists:map(fun prepare_aggregation/1, Aggregations));

handle('POST', [<<"aggregations">>], Req) ->
    Body = elli_request:body(Req),
    lager:debug("POST /control/aggregations: ~p", [Body]),

    case parse_json_of(Body, fun aggregation_from_json/1) of
        #aggregation_definition{}=Agg ->
            statser_config:add_aggregation(Agg),
            ok();
        {error, Error} ->
            bad_request(Error);
        error ->
            bad_request(<<"invalid aggregation definition given">>)
    end;

%---------------------------------------------------------------------
% SETTINGS API
%---------------------------------------------------------------------

handle('GET', [<<"settings">>, <<"ratelimits">>], _Req) ->
    RateLimits = statser_config:get_rate_limits(),
    rate_limits_to_json(RateLimits);


handle('POST', [<<"settings">>, <<"ratelimits">>], Req) ->
    Body = elli_request:body(Req),
    lager:debug("POST /control/settings/ratelimits: ~p", [Body]),

    case parse_json_of(Body, fun parse_rate_limits/1) of
        #rate_limit_config{creates_per_sec=Creates, updates_per_sec=Updates}=Ls ->
            % update config
            statser_config:set_rate_limits(Ls),

            % notify rate limiters
            statser_rate_limiter:change_limit(create_limiter, Creates),
            statser_rate_limiter:change_limit(update_limiter, Updates),

            rate_limits_to_json(Ls);
        {error, Error} ->
            bad_request(Error);
        _Error ->
            bad_request(<<"failed to parse rate limits">>)
    end;

%---------------------------------------------------------------------
% BASE API
%---------------------------------------------------------------------

handle('OPTIONS', _Path, _Req) ->
    {200, ?DEFAULT_HEADERS_CORS, <<"">>};

handle(_Method, _Path, _Req) ->
    {404, [], <<"not found">>}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

parse_rate_limits(Limits) when is_list(Limits) ->
    CurrentLimits = statser_config:get_rate_limits(),
    Parsed = lists:map(fun parse_rate_limit/1, Limits),

    lists:foldr(fun(_, error) -> error;
                   (_, {error, _}=E) -> E;
                   (error, _) -> error;
                   ({error, _}=E, _) -> E;
                   ({create, L}, Ls) ->
                        Ls#rate_limit_config{creates_per_sec=L};
                   ({update, L}, Ls) ->
                        Ls#rate_limit_config{updates_per_sec=L};
                   (_, _) -> error
                end, CurrentLimits, Parsed);

parse_rate_limits(_Limits) ->
    {error, <<"expecting list of rate limits">>}.


parse_rate_limit({Limit}) when is_list(Limit) ->
    Vs = [get_non_empty(<<"type">>, Limit),
          get_number(<<"limit">>, Limit, error)
         ],
    case validate(Vs) of
        [_, L] when L =< 0 orelse not is_integer(L) ->
            {error, <<"expecting positive integer">>};
        ["create", L] -> {create, L};
        ["update", L] -> {update, L};
        [_Unknown, _Limit] ->
            {error, <<"unknown rate limit type">>};
        [] -> error;
        Error -> Error
    end;

parse_rate_limit(_Limit) ->
    {error, <<"invalid rate limit given">>}.


rate_limits_to_json(#rate_limit_config{creates_per_sec=Creates, updates_per_sec=Updates}) ->
    Limits = [{[{<<"type">>, <<"create">>},
                {<<"limit">>, Creates}
               ]},
              {[{<<"type">>, <<"update">>},
                {<<"limit">>, Updates}
               ]}
             ],
    json(Limits).


parse_node_info({Nodes}) when is_list(Nodes) ->
    case proplists:get_value(<<"node">>, Nodes) of
        undefined ->
            {error, <<"missing 'node'">>};
        Node when is_binary(Node) ->
            {ok, prepare_node(Node)};
        _Otherwise ->
            {error, <<"invalid 'node' data">>}
    end;

parse_node_info(_Json) ->
    {error, <<"invalid JSON">>}.


prepare_node(Node) ->
    Node0 = case binary:match(Node, <<"@">>) of
                nomatch -> <<"statser@", Node/binary>>;
                _Otherwise -> Node
            end,

    % not too sure if I am happy with this `list_to_atom`
    list_to_atom(binary_to_list(Node0)).


prepare_storage(#storage_definition{name=Name, raw=Raw, retentions=Rs}) ->
    Retentions = lists:map(fun(Ret) ->
                                   list_to_binary(Ret#retention_definition.raw)
                           end, Rs),
    {[{<<"name">>, list_to_binary(Name)},
      {<<"pattern">>, list_to_binary(Raw)},
      {<<"retentions">>, Retentions}
     ]}.


prepare_aggregation(#aggregation_definition{name=Name, raw=Raw, aggregation=Agg, factor=Factor}) ->
    {[{<<"name">>, list_to_binary(Name)},
      {<<"pattern">>, list_to_binary(Raw)},
      {<<"aggregation">>, Agg},
      {<<"factor">>, Factor}
     ]}.


parse_json_of(Body, Func) ->
    try
        Json = jiffy:decode(Body),
        Func(Json)
    catch
        _Error -> {error, <<"malformed JSON">>}
    end.


storage_from_json({Json}) when is_list(Json) ->
    Vs = [get_non_empty(<<"name">>, Json),
          get_non_empty(<<"pattern">>, Json),
          get_string_list(<<"retentions">>, Json)
         ],

    case validate(Vs) of
        [Name, Pattern, Retentions] ->
            Rs0 = statser_config:parse_retentions(Retentions),
            case {re:compile(Pattern, [no_auto_capture]), Rs0} of
                {_, []} ->
                    {error, <<"invalid retentions given">>};
                {{ok, Regex}, Rs} ->
                    #storage_definition{
                       name=Name,
                       raw=Pattern,
                       pattern=Regex,
                       retentions=Rs};
                _Error -> {error, <<"invalid pattern expression">>}
            end;
        [] -> error;
        Error -> Error
    end;

storage_from_json(_Invalid) ->
    {error, <<"invalid JSON">>}.


aggregation_from_json({Json}) when is_list(Json) ->
    Vs = [get_non_empty(<<"name">>, Json),
          get_non_empty(<<"pattern">>, Json),
          statser_config:parse_aggregation(get_non_empty(<<"aggregation">>, Json)),
          get_number(<<"factor">>, Json, 0.5)
         ],

    case validate(Vs) of
        [Name, Pattern, Agg, Factor] ->
            case re:compile(Pattern, [no_auto_capture]) of
                {ok, Regex} ->
                    #aggregation_definition{
                       name=Name,
                       raw=Pattern,
                       pattern=Regex,
                       aggregation=Agg,
                       factor=Factor};
                _Error -> {error, <<"invalid pattern expression">>}
            end;
        [] -> error;
        Error -> Error
    end;

aggregation_from_json(_Invalid) ->
    {error, <<"invalid JSON">>}.


validate(Values) ->
    % we are searching for any errors while simply
    % keeping the first one found if any
    lists:foldr(fun(_, error) -> error;
                   (_, {error, _Reason}=E) -> E;
                   (error, _) -> error;
                   ({error, _Reason}=E, _) -> E;
                   (Result, Rs) -> [Result | Rs]
                end, [], Values).


get_string_list(Key, Input) ->
    case proplists:get_value(Key, Input, error) of
        error -> error;
        [] -> error;
        Vs ->
            case lists:all(fun is_binary/1, Vs) of
                true -> lists:map(fun binary_to_list/1, Vs);
                false -> error
            end
    end.


get_non_empty(Key, Input) ->
    case proplists:get_value(Key, Input, error) of
        [] -> error;
        <<>> -> error;
        Value when is_binary(Value) -> binary_to_list(Value);
        _Otherwise -> error
    end.


get_number(Key, Input, Default) ->
    case proplists:get_value(Key, Input, Default) of
        Value when is_number(Value) -> Value;
        _Otherwise -> error
    end.


-spec connection_state(node_status()) -> connected | disconnected.
connection_state(connected) -> connected;
connection_state(me) -> connected;
connection_state(_State) -> disconnected.


node_to_json(#node_info{node=Node, state=State}) ->
    IsSelf = State == me,

    {[{<<"node">>, Node},
      {<<"state">>, connection_state(State)},
      {<<"self">>, IsSelf}]}.


bad_request(Error) ->
    ErrorData = {[{<<"success">>, false},
                  {<<"message">>, Error}]},
    {400, ?DEFAULT_HEADERS_CORS, jiffy:encode(ErrorData)}.


json(Data) ->
    Json = jiffy:encode(Data),
    {200, ?DEFAULT_HEADERS_CORS, Json}.


ok() ->
    json({[{<<"success">>, true}]}).


%%
%% TESTS
%%

-ifdef(TEST).

storage_from_json_test_() ->
    [?_assertEqual(error, storage_from_json({[{<<"name">>, <<"test">>}]})),
     ?_assertEqual({error, <<"invalid pattern expression">>}, storage_from_json({[{<<"name">>, <<"test">>},
                                              {<<"pattern">>, <<"**">>},
                                              {<<"retentions">>, [<<"60:7d">>]}
                                             ]})),
     ?_assertMatch(#storage_definition{}, storage_from_json({[{<<"name">>, <<"test">>},
                                                              {<<"pattern">>, <<"abc">>},
                                                              {<<"retentions">>, [<<"60:7d">>]}
                                                             ]}))
    ].

aggregation_from_json_test_() ->
    [?_assertEqual(error, aggregation_from_json({[{<<"name">>, <<"test">>}]})),
     ?_assertEqual({error, <<"invalid pattern expression">>}, aggregation_from_json({[{<<"name">>, <<"test">>},
                                                  {<<"pattern">>, <<"**">>}]})),
     ?_assertMatch(#aggregation_definition{}, aggregation_from_json({[{<<"name">>, <<"test">>},
                                                                      {<<"pattern">>, <<"abc">>}]}))
    ].

parse_rate_limit_test_() ->
    [?_assertEqual({error, <<"invalid rate limit given">>}, parse_rate_limit([])),
     ?_assertEqual(error, parse_rate_limit({[]})),
     ?_assertEqual(error, parse_rate_limit({[{<<"type">>,<<"smth">>}]})),
     ?_assertEqual(error, parse_rate_limit({[{<<"type">>,<<"create">>}]})),
     ?_assertEqual({create, 1}, parse_rate_limit({[{<<"type">>,<<"create">>}, {<<"limit">>, 1}]})),
     ?_assertEqual({update, 24}, parse_rate_limit({[{<<"type">>,<<"update">>}, {<<"limit">>, 24}]})),
     ?_assertEqual({error, <<"expecting positive integer">>}, parse_rate_limit({[{<<"type">>,<<"update">>}, {<<"limit">>, -4}]})),
     ?_assertEqual({error, <<"expecting positive integer">>}, parse_rate_limit({[{<<"type">>,<<"update">>}, {<<"limit">>, 21.22}]}))
    ].

-endif.
