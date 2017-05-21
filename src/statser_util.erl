-module(statser_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-export([ceiling/1,
         floor/1]).


floor(X) when X < 0 ->
    Truncated = trunc(X),
    case X - Truncated == 0 of
       true -> Truncated;
       false -> Truncated - 1
    end;
floor(X) ->
    trunc(X).


ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    Truncated = trunc(X),
    case X - Truncated == 0 of
       true -> Truncated;
       false -> Truncated + 1
    end.
