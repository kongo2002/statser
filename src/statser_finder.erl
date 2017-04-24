-module(statser_finder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([find_metrics/1,
         find_metrics/2]).


find_metrics(Parts) ->
    find_metrics(Parts, ["."]).


find_metrics([FilePart], Cwd) ->
    Files = glob_whispers(FilePart, full_dir(Cwd)),
    MetricsPath = tl(Cwd),
    lists:map(fun(F) -> MetricsPath ++ [F] end, Files);

find_metrics([DirPart | Rest], Cwd) ->
    DirCandidates = glob_dirs(DirPart, full_dir(Cwd)),
    Descend = fun(Dir) -> find_metrics(Rest, Cwd ++ [Dir]) end,
    lists:flatmap(Descend, DirCandidates).


glob_dirs(Dir, Cwd) when is_binary(Dir) ->
    glob_dirs(binary_to_list(Dir), Cwd);

glob_dirs(Dir, Cwd) ->
    [D || D <- filelib:wildcard(Dir, Cwd),
          filelib:is_dir(Cwd ++ "/" ++ D)].


glob_whispers(File, Cwd) when is_binary(File) ->
    glob_whispers(binary_to_list(File), Cwd);

glob_whispers(File, Cwd) ->
    WhisperFile = File ++ ".wsp",
    [drop_suffix(F) || F <- filelib:wildcard(WhisperFile, Cwd)].


full_dir(Files) ->
    string:join(Files, "/").


drop_suffix(File) ->
    % TODO: oh god...
    lists:reverse(tl(tl(tl(tl(lists:reverse(File)))))).


%%
%% TESTS
%%

-ifdef(TEST).

drop_suffix_test_() ->
    [?_assertEqual("foo", drop_suffix("foo.wsp")),
     ?_assertEqual("foo bar", drop_suffix("foo bar.WSP"))].

-endif.
