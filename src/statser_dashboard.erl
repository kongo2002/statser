-module(statser_dashboard).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("api.hrl").
-include("statser.hrl").

-export([handle/3]).

-define(DOCROOT, <<"assets">>).


% root request
handle('GET', [], _Req) ->
    file:read_file(filename:join([?DOCROOT, "index.html"]));

% request metrics snapshot - 'pull-style' API
handle('GET', [<<"metrics">>], _Req) ->
    Metrics = statser_health:metrics(),
    {200, ?DEFAULT_HEADERS_CORS, Metrics};

% open SSE stream
handle('GET', [<<"stream">>], Req) ->
    statser_health:subscribe(elli_request:chunk_ref(Req)),

    {chunk, [{<<"Content-Type">>, <<"text/event-stream">>}]};

% arbitrary file request
handle('GET', Path, _Req) ->
    Filepath = filename:join([?DOCROOT | Path]),
    valid_path(Filepath) orelse throw({403, [], <<"Permission denied">>}),


    case file:read_file(Filepath) of
        {ok, Bin} ->
            {ok, Bin};
        {error, enoent} ->
            {404, <<"not found">>}
    end;

handle(_, _, _Req) ->
    {404, [], <<"not found">>}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

valid_path(Path) ->
    case binary:match(Path, <<"..">>) of
        {_, _} -> false;
        nomatch -> true
    end.
