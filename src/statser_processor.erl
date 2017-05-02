-module(statser_processor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([fetch_data/2]).

-define(TIMEOUT, 2000).


fetch_data(Paths, {From, Until, _MaxDataPoints}) ->
    % get metrics handler
    case ets:lookup(metrics, Paths) of
        [] -> [];
        [{_Path, Pid}] ->
            Data = gen_server:call(Pid, {fetch, From, Until}, ?TIMEOUT),
            % XXX: process max-data-points in here?
            Data
    end.
