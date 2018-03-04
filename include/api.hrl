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
