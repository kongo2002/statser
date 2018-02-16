-define(NO_CACHE, <<"no-cache">>).

-define(DEFAULT_HEADERS,
        [{<<"Pragma">>, ?NO_CACHE},
         {<<"Cache-Control">>, ?NO_CACHE},
         {<<"Content-Type">>, <<"application/json; charset=utf-8">>}]).

-define(DEFAULT_HEADERS_CORS,
        [{<<"Pragma">>, ?NO_CACHE},
         {<<"Cache-Control">>, ?NO_CACHE},
         {<<"Content-Type">>, <<"application/json; charset=utf-8">>},
         {<<"Access-Control-Allow-Origin">>, <<"*">>}]).
