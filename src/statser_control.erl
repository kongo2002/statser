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
    % TODO
    json([]);

handle('POST', [<<"nodes">>], Req) ->
    Body = elli_request:body(Req),
    lager:debug("/control/nodes: ~p", [Body]),

    Json = jiffy:decode(Body),
    case parse_node_info(Json) of
        {ok, Node} ->
            % TODO
            ok();
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
            {ok, Node};
        _Otherwise ->
            {error, <<"invalid 'node' data">>}
    end;

parse_node_info(_Json) ->
    {error, <<"invalid json">>}.


bad_request(Error) ->
    ErrorData = {[{<<"success">>, false},
                  {<<"message">>, Error}]},
    {400, ?DEFAULT_HEADERS, jiffy:encode(ErrorData)}.


json(Data) ->
    Json = jiffy:encode(Data),
    {200, ?DEFAULT_HEADERS, Json}.


ok() ->
    json({[{<<"success">>, true}]}).
