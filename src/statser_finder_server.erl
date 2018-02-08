-module(statser_finder_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("kernel/include/file.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([find_metrics/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(FINDER_UPDATE_INTERVAL, 10000).
-define(FINDER_SPECIAL_CHARS, [<<"*">>, <<"?">>, <<"{">>, <<"}">>]).

-record(state, {metrics, status=none, data_dir, timer, count}).

-record(metric_file, {
          name :: nonempty_string(),
          file :: nonempty_string(),
          handler=undefined :: pid() | undefined
         }).

-record(metric_dir, {
          name :: nonempty_string(),
          metrics :: orddict:orddict(),
          dirs :: orddict:orddict()
         }).

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
    {ok, #state{data_dir=DataDir}}.

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
    {noreply, State#state{metrics=Ms, count=Count, timer=Timer, status=none}};

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


find_metrics(Paths) ->
    PreparedPaths = prepare_paths(Paths),
    gen_server:call(?MODULE, {find_metrics, PreparedPaths}, 1000).


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


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
            {[], 0}
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

convert_metric_test_() ->
    [?_assertEqual({<<"foo.bar">>, undefined},
                   convert_metric(#metric_file{name = <<"bar">>}, [<<"foo">>])),
     ?_assertEqual({<<"eggs.ham.foo.bar">>, undefined},
                   convert_metric(#metric_file{name = <<"bar">>}, [<<"foo">>, <<"ham">>, <<"eggs">>]))
    ].

-endif.
