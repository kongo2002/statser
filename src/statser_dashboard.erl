-module(statser_dashboard).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([handle/3]).

-define(DOCROOT, <<"assets">>).


handle('GET', [], Req) ->
    file:read_file(filename:join([?DOCROOT, "index.html"]));

handle('GET', [<<"stream">>], Req) ->
    statser_instrumentation:add_subscriber(elli_request:chunk_ref(Req)),

    {chunk, [{<<"Content-Type">>, <<"text/event-stream">>}]};

handle('GET', Path, Req) ->
    Filepath = filename:join([?DOCROOT | Path]),
    valid_path(Filepath) orelse throw({403, [], <<"Permission denied">>}),


    case file:read_file(Filepath) of
        {ok, Bin} ->
            {ok, Bin};
        {error, enoent} ->
            {404, <<"Not found">>}
    end;

handle(_, _, _Req) ->
    {404, [], <<"not found">>}.


valid_path(Path) ->
    case binary:match(Path, <<"..">>) of
        {_, _} -> false;
        nomatch -> true
    end.
