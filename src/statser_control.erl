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

handle('GET', [<<"nodes">>], _Req) ->
    Nodes = statser_discoverer:get_nodes(),
    Nodes0 = lists:map(fun node_to_json/1, Nodes),
    json(Nodes0);

handle('POST', [<<"nodes">>], Req) ->
    Body = elli_request:body(Req),
    lager:debug("/control/nodes: ~p", [Body]),

    Json = jiffy:decode(Body),
    case parse_node_info(Json) of
        {ok, Node} ->
            case statser_discoverer:connect(Node) of
                true -> ok();
                _Else -> bad_request(<<"failed to connect">>)
            end;
        {error, Error} ->
            bad_request(Error)
    end;

handle(_Method, _Path, _Req) ->
    {404, [], <<"not found">>}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    {error, <<"invalid json">>}.


prepare_node(Node) ->
    Node0 = case binary:match(Node, <<"@">>) of
                nomatch -> <<"statser@", Node/binary>>;
                _Otherwise -> Node
            end,

    % not too sure if I am happy with this `list_to_atom`
    list_to_atom(binary_to_list(Node0)).


node_to_json(#node_info{node=Node}) ->
    {[{<<"node">>, Node}]}.


bad_request(Error) ->
    ErrorData = {[{<<"success">>, false},
                  {<<"message">>, Error}]},
    {400, ?DEFAULT_HEADERS, jiffy:encode(ErrorData)}.


json(Data) ->
    Json = jiffy:encode(Data),
    {200, ?DEFAULT_HEADERS, Json}.


ok() ->
    json({[{<<"success">>, true}]}).
