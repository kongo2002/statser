-module(statser_finder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([find_metrics/1,
         find_metrics/2,
         find_metrics_tree/1,
         find_metrics_tree/2]).


find_metrics_tree(Parts) ->
    Base = binary:bin_to_list(statser_config:get_data_dir()),
    find_metrics_tree(Parts, [Base]).

find_metrics_tree(Parts, Cwd) ->
    find_metrics(Parts, Cwd, true).


find_metrics(Parts) ->
    Base = binary:bin_to_list(statser_config:get_data_dir()),
    find_metrics(Parts, [Base]).

find_metrics(Parts, Cwd) ->
    find_metrics(Parts, Cwd, false).


find_metrics([FilePart], Cwd, WithDirs) ->
    MetricsPath = tl(Cwd),
    Files = glob_whispers(FilePart, full_dir(Cwd)),

    % fetch directory entries as well (if requested)
    Elements0 = case WithDirs of
                    true -> Files ++ glob_dirs(FilePart, full_dir(Cwd));
                    false -> Files
                end,

    Elements = lists:sort(fun by_name/2, Elements0),
    lists:map(fun(E) -> to_node(E, MetricsPath) end, Elements);

find_metrics([DirPart | Rest], Cwd, WithDirs) ->
    DirCandidates = glob_dirs(DirPart, full_dir(Cwd)),
    Descend = fun({dir, Dir}) -> find_metrics(Rest, Cwd ++ [Dir], WithDirs) end,
    lists:flatmap(Descend, DirCandidates).


to_node({file, File}, Path) ->
    to_node(File, Path, true);

to_node({dir, Dir}, Path) ->
    to_node(Dir, Path, false).


to_node(Node, Path, IsLeaf) ->
    {[{<<"leaf">>, IsLeaf},
      {<<"allowChildren">>, not IsLeaf},
      {<<"expandable">>, not IsLeaf},
      {<<"text">>, list_to_binary(Node)},
      {<<"id">>, to_path(Path ++ [Node])}]}.


to_path(Path) ->
    list_to_binary(string:join(Path, ".")).


by_name({_TypeA, NameA}, {_TypeB, NameB}) when NameA =< NameB -> true;
by_name({_TypeA, _NameA}, {_TypeB, _NameB}) -> false.


glob_dirs(Dir, Cwd) when is_binary(Dir) ->
    glob_dirs(binary_to_list(Dir), Cwd);

glob_dirs(Dir, Cwd) ->
    [{dir, D} || D <- filelib:wildcard(Dir, Cwd),
                 filelib:is_dir(Cwd ++ "/" ++ D)].


glob_whispers(File, Cwd) when is_binary(File) ->
    glob_whispers(binary_to_list(File), Cwd);

glob_whispers(File, Cwd) ->
    WhisperFile = File ++ ".wsp",
    [{file, drop_suffix(F)} || F <- filelib:wildcard(WhisperFile, Cwd)].


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
