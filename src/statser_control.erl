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

    case parse_node_info(Body) of
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

    case parse_node_info(Body) of
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

%---------------------------------------------------------------------
% AGGREGATION API
%---------------------------------------------------------------------

handle('GET', [<<"aggregations">>], _Req) ->
    Aggregations = statser_config:get_aggregations(),
    json(lists:map(fun prepare_aggregation/1, Aggregations));

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

parse_node_info(Binary) ->
    try
        Json = jiffy:decode(Binary),
        parse_node_info0(Json)
    catch
        _Error -> {error, <<"malformed JSON">>}
    end.


parse_node_info0({Nodes}) when is_list(Nodes) ->
    case proplists:get_value(<<"node">>, Nodes) of
        undefined ->
            {error, <<"missing 'node'">>};
        Node when is_binary(Node) ->
            {ok, prepare_node(Node)};
        _Otherwise ->
            {error, <<"invalid 'node' data">>}
    end;

parse_node_info0(_Json) ->
    {error, <<"invalid JSON">>}.


prepare_node(Node) ->
    Node0 = case binary:match(Node, <<"@">>) of
                nomatch -> <<"statser@", Node/binary>>;
                _Otherwise -> Node
            end,

    % not too sure if I am happy with this `list_to_atom`
    list_to_atom(binary_to_list(Node0)).


prepare_storage(#storage_definition{name=Name, retentions=Rs}) ->
    Retentions = lists:map(fun(Ret) ->
                                   list_to_binary(Ret#retention_definition.raw)
                           end, Rs),
    {[{<<"name">>, list_to_binary(Name)},
      {<<"retentions">>, Retentions}
     ]}.


prepare_aggregation(#aggregation_definition{name=Name, aggregation=Agg, factor=Factor}) ->
    {[{<<"name">>, list_to_binary(Name)},
      {<<"aggregation">>, Agg},
      {<<"factor">>, Factor}
     ]}.


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
