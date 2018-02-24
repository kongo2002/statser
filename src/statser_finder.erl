-module(statser_finder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("kernel/include/file.hrl").

-behaviour(gen_server).

-include("statser.hrl").

%% API
-export([start_link/0]).

-export([find_metrics/1,
         find_metrics_tree/1,
         processor_loop/1,
         register_metric_handler/2,
         unregister_metric_handler/1,
         register_remote/1,
         unregister_remote/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-type local_handler() :: {local, pid()}.

-type remote_handler() :: {remote, node(), pid()}.

-type handler() :: local_handler() | remote_handler() | undefined.

-record(metric_file, {
          name :: binary(),
          file :: nonempty_string() | undefined,
          handler :: handler()
         }).

-record(metric_dir, {
          name :: binary() | undefined,
          metrics :: orddict:orddict(),
          dirs :: orddict:orddict()
         }).

-record(state, {metrics, data_dir, processor, remotes}).

-define(FINDER_UPDATE_MILLIS_MIN, 300000).
-define(FINDER_UPDATE_MILLIS_PER_METRIC, 50).
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


-spec find_metrics([binary()]) -> [{binary(), pid() | undefined}].
find_metrics(Path) ->
    PreparedPath = prepare_path_components(Path),

    % TODO: what happens on timeout exactly, again..?
    gen_server:call(?MODULE, {find_metrics, PreparedPath, false}, 1000).


-spec find_metrics_tree([binary()]) -> [{binary(), tuple()}].
find_metrics_tree(Path) ->
    PreparedPath = prepare_path_components(Path),

    % TODO: what happens on timeout exactly, again..?
    gen_server:call(?MODULE, {find_metrics, PreparedPath, true}, 1000).


-spec register_metric_handler(binary(), handler()) -> ok.
register_metric_handler(Path, Pid) ->
    {Paths, Name} = metric_and_paths(Path),
    Metric = #metric_file{name=Name, handler=Pid},
    gen_server:cast(?MODULE, {register_handler, Metric, Paths}).


-spec unregister_metric_handler(binary()) -> ok.
unregister_metric_handler(Path) ->
    register_metric_handler(Path, undefined).


-spec register_remote(node()) -> ok.
register_remote(Node) ->
    gen_server:cast(?MODULE, {register_remote, Node}).


-spec unregister_remote(node()) -> ok.
unregister_remote(Node) ->
    gen_server:cast(?MODULE, {unregister_remote, Node}).


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
    Processor = spawn_link(?MODULE, processor_loop, [self()]),

    gen_server:cast(self(), prepare),

    {ok, #state{metrics=?EMPTY_METRIC_DIR,
                data_dir=DataDir,
                processor=Processor,
                remotes=sets:new()}}.

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
handle_call({find_metrics, Paths, GlobDirs}, _From, State) ->
    Metrics = State#state.metrics,
    Result = find_metrics(Paths, Metrics, GlobDirs),
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
handle_cast(prepare, #state{data_dir=Dir, processor=Processor}=State) ->
    lager:debug("start initializing metrics finder"),

    % tigger initial metric lookup
    Processor ! {update_metrics, Dir},

    % subscribe to events
    statser_event:start_link(fun on_event/1),

    {noreply, State};

handle_cast({register_handler, Metric, Paths}=Msg, State) ->
    % trigger local handling
    State#state.processor ! Msg,

    % in case of a local handler we publish this one to all
    % known remote nodes
    case Metric of
        #metric_file{handler={local, Pid}} ->
            sets:fold(fun(Node, _) ->
                              M = Metric#metric_file{handler={remote, node(), Pid}},
                              gen_server:cast({?MODULE, Node}, {register_handler, M, Paths})
                      end, ok, State#state.remotes);
        _Otherwise ->
            lager:debug("finder: registering a remote handler ~p", [Metric]),
            ok
    end,

    {noreply, State};

handle_cast({register_remote, Node}, State) ->
    lager:info("finder: registered new node ~p", [Node]),

    Remotes = sets:add_element(Node, State#state.remotes),

    % request new node's metrics to be merged with ours
    {statser_finder, Node} ! {get_metrics, self()},

    {noreply, State#state{remotes=Remotes}};

handle_cast({unregister_remote, Node} = Msg, State) ->
    Remotes = sets:del_element(Node, State#state.remotes),

    % remove metrics of that remote node
    State#state.processor ! Msg,

    {noreply, State#state{remotes=Remotes}};

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
handle_info({finder_result, Metrics}, State) ->
    {noreply, State#state{metrics=Metrics}};

handle_info({get_metrics, _} = Msg, State) ->
    % dispatch remote metrics merge to processor
    State#state.processor ! Msg,
    {noreply, State};

handle_info({remote_metrics, _, _} = Msg, State) ->
    % dispatch remote metrics merge to processor
    State#state.processor ! Msg,
    {noreply, State};

handle_info(Info, State) ->
    lager:warning("finder: received unexpected message: ~p", [Info]),
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
terminate(_Reason, _State) ->
    lager:info("terminating finder server at ~w", [self()]),
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

% internal receive-loop that handles all potentially 'long' running
% tasks of the 'finder' so that the main metrics finder service is
% as responsive as possible
% the processing loop asynchronously maintains the main metrics data
% structure and reports back to the parent server with the results
processor_loop(Parent) ->
    lager:info("spawning asynchronous finder processor at ~p", [self()]),
    processor_loop(Parent, ?EMPTY_METRIC_DIR, 0).

processor_loop(Parent, Metrics, Count) ->
    receive
        % register new metrics handler
        {register_handler, Metric, Paths} ->
            NewMetrics = update_or_insert_metric(Paths, Metric, Metrics),
            Parent ! {finder_result, NewMetrics},

            processor_loop(Parent, NewMetrics, Count);

        % search local file system for metrics files and integrate
        % those in the metrics data structure
        {update_metrics, Dir} ->
            {NewMetrics, NewCount} = update_metrics_files(Dir, Count),
            Merged = merge_metric_dir(Metrics, NewMetrics),
            Parent ! {finder_result, Merged},

            % schedule next metrics lookup depending on the current number
            % of tracked metrics (minimum: 5 min)
            UpdateIn = max(NewCount * ?FINDER_UPDATE_MILLIS_PER_METRIC, ?FINDER_UPDATE_MILLIS_MIN),
            erlang:send_after(UpdateIn, self(), {update_metrics, Dir}),

            processor_loop(Parent, Merged, NewCount);

        % prepare local metrics structure to be sent to another remote node
        {get_metrics, From} ->
            ForRemote = prepare_metrics_for_remote(Metrics),
            From ! {remote_metrics, node(), ForRemote},
            processor_loop(Parent, Metrics, Count);

        % unregister all metrics handler of a specific remote node
        {unregister_remote, Remote} ->
            lager:debug("remove metrics from remote ~p", [Remote]),

            WithoutRemotes = remove_remotes(Remote, Metrics),
            Parent ! {finder_result, WithoutRemotes},
            processor_loop(Parent, WithoutRemotes, Count);

        % integrate the metrics of another remote node into our local
        % metrics data structure so that we are able to directly address
        % 'foreign' metrics via the associated remote node
        {remote_metrics, RemoteNode, RemoteMetrics} ->
            lager:debug("processing metrics from remote node ~p", [RemoteNode]),

            Merged = merge_metric_dir(Metrics, RemoteMetrics),
            Parent ! {finder_result, Merged},

            processor_loop(Parent, Merged, Count);

        Unhandled ->
            lager:error("unexpected message in finder processor: ~p", [Unhandled]),
            error
    end.


convert_to_remote(Node, #metric_file{handler={local, Pid}}=MF, Ms) ->
    Handler = {remote, node(), Pid},
    V0 = MF#metric_file{handler=Handler},
    orddict:store(Node, V0, Ms);

convert_to_remote(_Node, _MetricFile, Metrics) ->
    Metrics.


% before sending our local metrics structure we want to
% prepare it for the target/remote node, meaning
%   * filter out non-local metrics
%   * filter out metrics w/o handler
prepare_metrics_for_remote(#metric_dir{metrics=Metrics, dirs=Dirs}=Dir) ->
    Ms0 = orddict:fold(fun convert_to_remote/3, orddict:new(), Metrics),
    Ds0 = orddict:fold(fun(K, V, Ds) ->
                               V0 = prepare_metrics_for_remote(V),
                               case orddict:is_empty(V0#metric_dir.metrics) of
                                   false -> orddict:store(K, V0, Ds);
                                   true -> Ds
                               end
                       end, orddict:new(), Dirs),

    Dir#metric_dir{metrics=Ms0, dirs=Ds0}.


% this function is used on node disconnect/removal
% so that all metrics that reference that specific remote node
% are removed from the metrics structure
remove_remotes(Remote, #metric_dir{metrics=Metrics, dirs=Dirs}=Dir) ->
    Ms0 = orddict:fold(fun(K, V, Ms) ->
                               case V#metric_file.handler of
                                   {remote, Remote, _Pid} -> Ms;
                                   _Otherwise -> orddict:store(K, V, Ms)
                               end
                       end, orddict:new(), Metrics),

    Ds0 = orddict:fold(fun(K, V, Ds) ->
                               V0 = remove_remotes(Remote, V),
                               case orddict:is_empty(V0#metric_dir.metrics) of
                                   false -> orddict:store(K, V0, Ds);
                                   true -> Ds
                               end
                       end, orddict:new(), Dirs),

    Dir#metric_dir{metrics=Ms0, dirs=Ds0}.


on_event({connected, Node}) ->
    ?MODULE:register_remote(Node);

on_event({disconnected, Node}) ->
    ?MODULE:unregister_remote(Node);

on_event({down, Node}) ->
    % the 'finder' does not distinguish between an unavailable node
    % and a non-existing one (in contrast to the 'discoverer')
    ?MODULE:unregister_remote(Node);

on_event(_Event) -> ok.


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
%
% as of now we won't overwrite any local handlers with remote ones
merge_metric_file(#metric_file{handler={local, _}}=A,
                  #metric_file{handler={remote, _, _}}) -> A;

merge_metric_file(#metric_file{}=A, #metric_file{}=B) ->
    % as of now we will favor 'local' handler for 'remote' handlers
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


find_metrics(Paths, Dir, GlobDirs) ->
    find_metrics(Paths, Dir, [], GlobDirs).


find_metrics([error], _Dir, _Parents, _Glob) -> [];
find_metrics([all], #metric_dir{metrics=Ms}, Parents, false) ->
    lists:map(fun({_, M}) ->
                      convert_metric(M, Parents) end,
              orddict:to_list(Ms));

find_metrics([all], #metric_dir{metrics=Ms, dirs=Ds}, Parents, true) ->
    Ms0 = lists:map(fun({_, M}) ->
                            convert_metric(M, Parents, true) end,
                    orddict:to_list(Ms)),
    Ds0 = lists:map(fun({_, M}) ->
                            convert_dir(M, Parents) end,
                    orddict:to_list(Ds)),
    Ms0 ++ Ds0;

find_metrics([{glob, Path}], #metric_dir{metrics=Ms}, Parents, false) ->
    Filter = filter_by_pattern(Path),
    Filtered = lists:filter(Filter, orddict:to_list(Ms)),
    lists:map(fun({_K, V}) ->
                      convert_metric(V, Parents)
              end, Filtered);

find_metrics([{glob, Path}], #metric_dir{metrics=Ms, dirs=Ds}, Parents, true) ->
    Filter = filter_by_pattern(Path),
    MsFiltered = lists:filter(Filter, orddict:to_list(Ms)),
    Ms0 = lists:map(fun({_K, V}) ->
                      convert_metric(V, Parents, true)
              end, MsFiltered),
    DsFiltered = lists:filter(Filter, orddict:to_list(Ds)),
    Ds0 = lists:map(fun({_K, V}) ->
                            convert_dir(V, Parents)
              end, DsFiltered),
    Ms0 ++ Ds0;

find_metrics([{exact, Path}], #metric_dir{metrics=Ms}, Parents, false) ->
    case orddict:find(Path, Ms) of
        {ok, Metric} -> [convert_metric(Metric, Parents)];
        error -> []
    end;

find_metrics([{exact, Path}], #metric_dir{metrics=Ms, dirs=Ds}, Parents, true) ->
    Ms0 = case orddict:find(Path, Ms) of
              {ok, Metric} -> [convert_metric(Metric, Parents, true)];
              error -> []
          end,
    Ds0 = case orddict:find(Path, Ds) of
              {ok, Dir} -> [convert_dir(Dir, Parents)];
              error -> []
          end,
    Ms0 ++ Ds0;

find_metrics([error | _Paths], _Dir, _Parents, _Glob) -> [];
find_metrics([all | Paths], #metric_dir{dirs=Dirs}, Parents, Glob) ->
    lists:flatmap(fun({Name, D}) ->
                          find_metrics(Paths, D, [Name | Parents], Glob) end,
                  orddict:to_list(Dirs));

find_metrics([{glob, Path} | Paths], #metric_dir{dirs=Dirs}, Parents, Glob) ->
    Filter = filter_by_pattern(Path),
    lists:flatmap(fun({Name, D}=Dir) ->
                          case Filter(Dir) of
                              true -> find_metrics(Paths, D, [Name | Parents], Glob);
                              false -> []
                          end
                  end, orddict:to_list(Dirs));

find_metrics([{exact, Path} | Paths], #metric_dir{dirs=Dirs}, Parents, Glob) ->
    case orddict:find(Path, Dirs) of
        {ok, Dir} -> find_metrics(Paths, Dir, [Path | Parents], Glob);
        error -> []
    end.


convert_metric(Metric, Path) ->
    convert_metric(Metric, Path, false).

convert_metric(#metric_file{name=Name, handler=Handler}, Path, false) ->
    Path0 = to_path(Name, Path),
    {Path0, Handler};

convert_metric(#metric_file{name=Name}, Path, true) ->
    Path0 = to_path(Name, Path),
    Node = {[{<<"leaf">>, true},
             {<<"allowChildren">>, false},
             {<<"expandable">>, false},
             {<<"text">>, Name},
             {<<"id">>, Path0}]},
    {Name, Node}.


convert_dir(#metric_dir{name=Name}, Path) ->
    Path0 = to_path(Name, Path),
    Node = {[{<<"leaf">>, false},
             {<<"allowChildren">>, true},
             {<<"expandable">>, true},
             {<<"text">>, Name},
             {<<"id">>, Path0}]},
    {Name, Node}.


to_path(Name, Path) ->
    F = fun(A, <<>>) -> <<A/binary>>;
           (A, B) -> <<A/binary, ".", B/binary>>
        end,
    lists:foldl(F, <<>>, [Name | Path]).


prepare_path_components(Paths) ->
    lists:map(fun prepare_path_component/1, Paths).


prepare_path_component(<<"*">>) -> all;
prepare_path_component(Path) ->
    case binary:match(Path, ?FINDER_SPECIAL_CHARS) of
        nomatch ->
            {exact, Path};
        _Glob ->
            prepare_blob_pattern(Path)
    end.


filter_by_pattern(Pattern) ->
    fun({Key, _V}) ->
            case re:run(Key, Pattern) of
                {match, _} -> true;
                _Otherwise -> false
            end
    end.


prepare_blob_pattern(Glob) ->
    % TODO: consider memoization
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
    split_last(Parts).


split_last([]) -> error;
split_last(Parts) -> split_last(Parts, []).

split_last([Part], Acc) ->
    {lists:reverse(Acc), Part};
split_last([Part | Parts], Acc) ->
    split_last(Parts, [Part | Acc]).


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

prepare_path_component_test_() ->
    Type = fun({T, _}) -> T end,

    [?_assertEqual(all, prepare_path_component(<<"*">>)),
     ?_assertEqual({exact, <<"foo">>}, prepare_path_component(<<"foo">>)),
     ?_assertEqual({exact, <<"ham eggs">>}, prepare_path_component(<<"ham eggs">>)),
     ?_assertEqual(glob, Type(prepare_path_component(<<"foo*">>))),
     ?_assertEqual(glob, Type(prepare_path_component(<<"fo?">>))),
     ?_assertEqual(glob, Type(prepare_path_component(<<"* fo?">>))),
     ?_assertEqual(error, prepare_path_component(<<"foo*(">>)),
     ?_assertEqual(error, prepare_path_component(<<"*(">>)),
     ?_assertEqual(error, prepare_path_component(<<"fo? [">>))
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
