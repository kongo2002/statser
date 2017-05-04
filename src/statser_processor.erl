-module(statser_processor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([fetch_data/4]).

-define(TIMEOUT, 2000).


fetch_data(Paths, From, Until, Now) ->
    % TODO: asynchronous fetching would be really nice
    lists:map(fun (Path) ->
                      % get metrics handler
                      case ets:lookup(metrics, Paths) of
                          [] ->
                              % there is no metrics handler already meaning
                              % there is nothing cached to be merged
                              % -> just read from fs directly instead
                              File = statser_metric_handler:get_whisper_file(Path),
                              Result = statser_whisper:fetch(File, From, Until, Now),
                              {Path, Result};
                          [{_Path, Pid}] ->
                              Result = gen_server:call(Pid, {fetch, From, Until, Now}, ?TIMEOUT),
                              {Path, Result}
                      end
              end, Paths).
