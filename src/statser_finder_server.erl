-module(statser_finder_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("kernel/include/file.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([find_metrics/1,
         register_metric_handler/2,
         unregister_metric_handler/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {metrics, status=none, data_dir, timer, count}).

-record(metric_file, {
          name :: nonempty_string(),
          file :: nonempty_string(),
          handler=undefined :: pid() | undefined
         }).

-record(metric_dir, {
          name :: nonempty_string() | undefined,
          metrics :: orddict:orddict(),
          dirs :: orddict:orddict()
         }).

-define(FINDER_UPDATE_INTERVAL, 60000).
-define(FINDER_SPECIAL_CHARS, [<<"*">>, <<"?">>, <<"{">>, <<"}">>]).
-define(EMPTY_METRIC_DIR, #metric_dir{metrics=orddict:new(), dirs=orddict:new()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


find_metrics(Paths) ->
    PreparedPaths = prepare_paths(Paths),
    gen_server:call(?MODULE, {find_metrics, PreparedPaths}, 1000).


register_metric_handler(Path, Pid) ->
    {Paths, Name} = metric_and_paths(Path),
    Metric = #metric_file{name=Name, handler=Pid},
    gen_server:cast(?MODULE, {register_handler, Metric, Paths}).


unregister_metric_handler(Path) ->
    register_metric_handler(Path, undefined).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    DataDir = statser_config:get_data_dir(),
    gen_server:cast(self(), prepare),
    {ok, #state{metrics=?EMPTY_METRIC_DIR, data_dir=DataDir}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({find_metrics, Paths}, _From, State) ->
    Metrics = State#state.metrics,
    Result = find_metrics(Paths, Metrics),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(prepare, State) ->
    lager:debug("start initializing metrics finder"),
    State0 = spawn_update_metrics(State),
    {noreply, State0};

handle_cast({register_handler, Metric, Paths}, State) ->
    State0 = register_handler(Metric, Paths, State),
    {noreply, State0};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(update_metrics, State) ->
    State0 = spawn_update_metrics(State),
    {noreply, State0};

handle_info({finder_result, {Ms, Count}}, State) ->
    Timer = erlang:send_after(?FINDER_UPDATE_INTERVAL, self(), update_metrics),
    Merged = merge_metric_dir(State#state.metrics, Ms),

    {noreply, State#state{metrics=Merged, count=Count, timer=Timer, status=none}};

handle_info(Info, State) ->
    lager:warning("finder_server: received unexpected message: ~p", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{timer=undefined}) ->
    lager:info("terminating finder server at ~w", [self()]),
    ok;

terminate(_Reason, #state{timer=Timer}) ->
    lager:info("terminating finder server at ~w", [self()]),
    erlang:cancel_timer(Timer, [{async, true}]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

register_handler(Metric, Paths, #state{metrics=Ms}=State) ->
    Ms0 = update_or_insert_metric(Paths, Metric, Ms),
    State#state{metrics=Ms0}.


update_or_insert_metric([], NewMetric, #metric_dir{metrics=Ms}=M) ->
    Name = NewMetric#metric_file.name,
    Ms0 = orddict:update(Name,
                         fun(Metric) ->
                                 Pid = NewMetric#metric_file.handler,
                                 Metric#metric_file{handler=Pid} end,
                         NewMetric, Ms),
    M#metric_dir{metrics=Ms0};

update_or_insert_metric([Path | Paths]=P, NewMetric, #metric_dir{dirs=Dirs}=M) ->
    Dir0 = case orddict:find(Path, Dirs) of
               {ok, Dir} ->
                   update_or_insert_metric(Paths, NewMetric, Dir);
               error ->
                   new_metric_dir(P, NewMetric)
           end,

    Dirs0 = orddict:store(Path, Dir0, Dirs),
    M#metric_dir{dirs=Dirs0}.


new_metric_dir([Path], NewMetric) ->
    Name = NewMetric#metric_file.name,
    #metric_dir{name=Path,
                metrics=orddict:from_list([{Name, NewMetric}]),
                dirs=orddict:new()};

new_metric_dir([Path | Paths], NewMetric) ->
    NewDir = new_metric_dir(Paths, NewMetric),
    Insert = [{NewDir#metric_dir.name, NewDir}],
    #metric_dir{name=Path,
                metrics=orddict:new(),
                dirs=orddict:from_list(Insert)}.


% this function is supposed to merge two instances of a 'metric_file'
% into one by combining the set values of both
% if in doubt, the second metric_file is assumed to be 'the newest' one
merge_metric_file(#metric_file{}=A, #metric_file{}=B) ->
    Pid = case {A#metric_file.handler, B#metric_file.handler} of
              {PidA, undefined} -> PidA;
              {_, PidB} -> PidB
          end,
    Name = case {A#metric_file.name, B#metric_file.name} of
               {NameA, undefined} -> NameA;
               {_, NameB} -> NameB
           end,
    File = case {A#metric_file.file, B#metric_file.file} of
               {FileA, undefined} -> FileA;
               {_, FileB} -> FileB
           end,
    #metric_file{name=Name, file=File, handler=Pid}.


merge_metric_dir(#metric_dir{}=A, #metric_dir{}=B) ->
    Metrics = orddict:fold(fun(K, V, Ms) ->
                                   Update = fun(Existing) ->
                                                    merge_metric_file(Existing, V)
                                            end,
                                   orddict:update(K, Update, V, Ms)
                           end,
                           A#metric_dir.metrics,
                           B#metric_dir.metrics),

    Dirs = orddict:fold(fun(K, V, Ds) ->
                            Update = fun(Existing) ->
                                             merge_metric_dir(Existing, V)
                                     end,
                            orddict:update(K, Update, V, Ds)
                        end,
                        A#metric_dir.dirs,
                        B#metric_dir.dirs),

    A#metric_dir{metrics=Metrics, dirs=Dirs}.


find_metrics(Paths, Dir) ->
    find_metrics(Paths, Dir, []).


find_metrics([error], _Dir, _Parents) -> [];
find_metrics([all], #metric_dir{metrics=Ms}, Parents) ->
    lists:map(fun({_, M}) ->
                      convert_metric(M, Parents) end,
              orddict:to_list(Ms));

find_metrics([{glob, Path}], #metric_dir{metrics=Ms}, Parents) ->
    Filter = filter_by_pattern(Path),
    Filtered = lists:filter(Filter, orddict:to_list(Ms)),
    lists:map(fun({_K, V}) ->
                      convert_metric(V, Parents)
              end, Filtered);

find_metrics([{exact, Path}], #metric_dir{metrics=Ms}, Parents) ->
    case orddict:find(Path, Ms) of
        {ok, Metric} -> [convert_metric(Metric, Parents)];
        error -> []
    end;

find_metrics([error | _Paths], _Dir, _Parents) -> [];
find_metrics([all | Paths], #metric_dir{dirs=Dirs}, Parents) ->
    lists:flatmap(fun({Name, D}) ->
                          find_metrics(Paths, D, [Name | Parents]) end,
                  orddict:to_list(Dirs));

find_metrics([{glob, Path} | Paths], #metric_dir{dirs=Dirs}, Parents) ->
    Filter = filter_by_pattern(Path),
    lists:flatmap(fun({Name, D}=Dir) ->
                          case Filter(Dir) of
                              true -> find_metrics(Paths, D, [Name | Parents]);
                              false -> []
                          end
                  end, orddict:to_list(Dirs));

find_metrics([{exact, Path} | Paths], #metric_dir{dirs=Dirs}, Parents) ->
    case orddict:find(Path, Dirs) of
        {ok, Dir} -> find_metrics(Paths, Dir, [Path | Parents]);
        error -> []
    end.


convert_metric(#metric_file{name=Name, handler=Pid}, Path) ->
    F = fun(A, <<>>) -> <<A/binary>>;
           (A, B) -> <<A/binary, ".", B/binary>>
        end,
    Path0 = lists:foldl(F, <<>>, [Name | Path]),
    {Path0, Pid}.


prepare_paths(Paths) ->
    lists:map(fun prepare_path/1, Paths).


prepare_path(<<"*">>) -> all;
prepare_path(Path) ->
    case binary:match(Path, ?FINDER_SPECIAL_CHARS) of
        nomatch ->
            {exact, Path};
        _Glob ->
            prepare_blob_pattern(Path)
    end.


filter_by_pattern(Pattern) ->
    fun({Key, _V}) ->
            case re:run(Key, Pattern) of
                match -> true;
                {match, _} -> true;
                _Otherwise -> false
            end
    end.


prepare_blob_pattern(Glob) ->
    P0 = binary:replace(Glob, <<"*">>, <<".*">>, [global]),
    P1 = binary:replace(P0, <<"?">>, <<".">>, [global]),
    case re:compile(P1, [anchored, no_auto_capture]) of
        {ok, RE} -> {glob, RE};
        _Otherwise -> error
    end.


update_metrics_files(Dir, OldCount) ->
    Start = erlang:monotonic_time(millisecond),

    case find_metrics_files([], Dir) of
        [#metric_dir{}=Ms] ->
            Count = count_metrics(Ms),

            if OldCount /= Count ->
                   Duration = erlang:monotonic_time(millisecond) - Start,
                   lager:info("found ~p metrics in data directory '~s' [took ~p ms]", [Count, Dir, Duration]);
               true -> ok
            end,

            {Ms, Count};
        _Otherwise ->
            if OldCount /= 0 ->
                   lager:info("no metrics found in data directory '~s'", [Dir]);
               true -> ok
            end,
            {?EMPTY_METRIC_DIR, 0}
    end.

spawn_update_metrics(#state{status=pending}=State) ->
    lager:info("asynchronous metrics finder still running"),
    State;

spawn_update_metrics(#state{data_dir=Dir, count=OldCount}=State) ->
    lager:debug("spawning asynchronous metrics finder"),

    Self = self(),
    spawn_link(fun() -> Self ! {finder_result, update_metrics_files(Dir, OldCount)} end),

    State#state{status=pending}.


count_metrics(Metrics) ->
    count_metrics(none, Metrics, 0).

count_metrics(_K, #metric_dir{dirs=Dirs, metrics=Metrics}, Count) ->
    Count + orddict:size(Metrics) + orddict:fold(fun count_metrics/3, 0, Dirs).


find_metrics_files([$. | _], _Base) -> [];
find_metrics_files(File, Base) ->
    Path = filename:join(Base, File),
    case file:read_file_info(Path, [raw]) of
        {ok, #file_info{type=directory}} ->
            case file:list_dir(Path) of
                {ok, Contents} ->
                    F = fun(Name, {Fs, Ds}=Acc) ->
                                case find_metrics_files(Name, Path) of
                                    [] -> Acc;
                                    [#metric_file{name=N}=X] -> {[{N, X} | Fs], Ds};
                                    [#metric_dir{name=N}=X] -> {Fs, [{N, X} | Ds]}
                                end
                        end,
                    case lists:foldl(F, {[], []}, Contents) of
                        {[], []} -> [];
                        {Metrics, Dirs} ->
                            [#metric_dir{name=binary:list_to_bin(File),
                                         metrics=orddict:from_list(Metrics),
                                         dirs=orddict:from_list(Dirs)}]
                    end;
                _Otherwise -> []
            end;
        {ok, #file_info{type=regular, access=read_write}} ->
            case get_suffix(File) of
                {Name, ".wsp"} ->
                    BinName = binary:list_to_bin(Name),
                    [#metric_file{name=BinName, file=File}];
                _Otherwise -> []
            end;
        _Otherwise -> []
    end.


metric_and_paths(Metric) ->
    Parts = statser_util:split_metric(Metric),
    keep_last(Parts).


keep_last([]) -> error;
keep_last(Parts) -> keep_last(Parts, []).

keep_last([Part], Acc) ->
    {lists:reverse(Acc), Part};
keep_last([Part | Parts], Acc) ->
    keep_last(Parts, [Part | Acc]).


get_suffix(File) ->
    get_suffix(lists:reverse(File), 4, []).

get_suffix([], _X, Suffix) ->
    % string:lowercase since OTP 20
    {lists:reverse([]), string:to_lower(Suffix)};
get_suffix(File, 0, Suffix) ->
    % string:lowercase since OTP 20
    {lists:reverse(File), string:to_lower(Suffix)};
get_suffix([H | Ts], X, Suffix) ->
    get_suffix(Ts, X-1, [H | Suffix]).


-ifdef(TEST).

get_suffix_test_() ->
    [?_assertEqual({[], []}, get_suffix([])),
     ?_assertEqual({"foo", ".wsp"}, get_suffix("foo.wsp")),
     ?_assertEqual({"", "sp"}, get_suffix("sp")),
     ?_assertEqual({"foo", ".wsp"}, get_suffix("foo.WSP")),
     ?_assertEqual({"Foo", ".wsp"}, get_suffix("Foo.WSP"))
    ].

metric_and_paths_test_() ->
    [?_assertEqual(error, metric_and_paths(<<"">>)),
     ?_assertEqual({[], <<"foo">>}, metric_and_paths(<<"foo">>)),
     ?_assertEqual({[<<"foo">>], <<"bar">>}, metric_and_paths(<<"foo.bar">>)),
     ?_assertEqual({[<<"foo">>, <<"bar">>], <<"test">>}, metric_and_paths(<<"foo.bar.test">>))
    ].

convert_metric_test_() ->
    [?_assertEqual({<<"foo.bar">>, undefined},
                   convert_metric(#metric_file{name = <<"bar">>},
                                  [<<"foo">>])),
     ?_assertEqual({<<"eggs.ham.foo.bar">>, undefined},
                   convert_metric(#metric_file{name = <<"bar">>},
                                  [<<"foo">>, <<"ham">>, <<"eggs">>]))
    ].

prepare_path_test_() ->
    Type = fun({T, _}) -> T end,

    [?_assertEqual(all, prepare_path(<<"*">>)),
     ?_assertEqual({exact, <<"foo">>}, prepare_path(<<"foo">>)),
     ?_assertEqual({exact, <<"ham eggs">>}, prepare_path(<<"ham eggs">>)),
     ?_assertEqual(glob, Type(prepare_path(<<"foo*">>))),
     ?_assertEqual(glob, Type(prepare_path(<<"fo?">>))),
     ?_assertEqual(glob, Type(prepare_path(<<"* fo?">>))),
     ?_assertEqual(error, prepare_path(<<"foo*(">>)),
     ?_assertEqual(error, prepare_path(<<"*(">>)),
     ?_assertEqual(error, prepare_path(<<"fo? [">>))
    ].

new_metric_dir_test_() ->
    Metric = #metric_file{name= <<"test">>},
    Exp1 = #metric_dir{name= <<"bar">>, dirs=[], metrics=orddict:from_list([{<<"test">>, Metric}])},
    Exp2 = #metric_dir{name= <<"foo">>, dirs=orddict:from_list([{<<"bar">>, Exp1}]), metrics=[]},

    [?_assertEqual(Exp2, new_metric_dir([<<"foo">>, <<"bar">>], Metric))
    ].

update_or_insert_metric_test_() ->
    EmptyDir = #metric_dir{name=[], metrics=orddict:new(), dirs=orddict:new()},
    Metric = #metric_file{name= <<"test">>},
    Exp1 = #metric_dir{name= <<"bar">>, dirs=[], metrics=orddict:from_list([{<<"test">>, Metric}])},
    Exp2 = #metric_dir{name= <<"foo">>, dirs=orddict:from_list([{<<"bar">>, Exp1}]), metrics=[]},
    Exp3 = EmptyDir#metric_dir{dirs=orddict:from_list([{<<"foo">>, Exp2}])},
    Exp4 = EmptyDir#metric_dir{dirs=orddict:from_list([{<<"foo">>, Exp2#metric_dir{metrics=orddict:from_list([{<<"test">>, Metric}])}}])},

    [?_assertEqual(Exp3, update_or_insert_metric([<<"foo">>, <<"bar">>], Metric, EmptyDir)),
     ?_assertEqual(Exp3, update_or_insert_metric([<<"foo">>, <<"bar">>], Metric, Exp3)),
     ?_assertEqual(Exp4, update_or_insert_metric([<<"foo">>], Metric, Exp3))
    ].

merge_metric_dir_test_() ->
    EmptyDir = #metric_dir{name=[], metrics=orddict:new(), dirs=orddict:new()},
    Metric = #metric_file{name= <<"test">>},
    Exp1 = #metric_dir{name= <<"bar">>, dirs=[], metrics=orddict:from_list([{<<"test">>, Metric}])},
    Exp2 = #metric_dir{name= <<"foo">>, dirs=orddict:from_list([{<<"bar">>, Exp1}]), metrics=[]},
    Exp3 = EmptyDir#metric_dir{dirs=orddict:from_list([{<<"foo">>, Exp2}])},

    [?_assertEqual(Exp3, merge_metric_dir(Exp3, Exp3))
    ].

-endif.