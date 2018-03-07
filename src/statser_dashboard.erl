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
    {301, [{<<"Location">>, <<"/.statser/index.html">>}], []};

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
