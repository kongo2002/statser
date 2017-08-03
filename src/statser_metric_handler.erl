-module(statser_metric_handler).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
         get_whisper_file/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(EPOCH_SECONDS_2000, 946681200).

-define(MIN_CACHE_ENTRIES, 5).

-record(state, {path, dirs, file, fspath, metadata, cache=[], cache_size=0}).

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
start_link(Path) ->
    gen_server:start_link(?MODULE, Path, []).

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
init(Path) ->
    lager:info("initializing metric handler for path ~p [~w]", [Path, self()]),

    % let's try to register ourselves
    case ets:insert_new(metrics, {Path, self()}) of
        true ->
            % we could register ourself, let's continue
            % TODO: not quite sure of this additional `prepare` step
            %       we could do that in here as well...
            gen_server:cast(self(), prepare),
            {ok, #state{path=Path}};
        false ->
            % there is a metric handler already for this path
            % let's shut down now
            {stop, normal}
    end.

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
handle_call({fetch, From, Until, Now}, _From, State) ->
    File = State#state.fspath,
    case statser_whisper:fetch(File, From, Until, Now) of
        #series{} = Result ->
            Cache = State#state.cache,
            Merged = merge_with_cache(Result, Cache),
            {reply, Merged, State};
        Error ->
            {reply, Error, State}
    end;

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
    % first we have to determine the proper path and file to use
    {ok, Dirs, File} = get_directory(State#state.path),
    Path = prepare_file(Dirs, File),
    lager:info("prepared path: ~p", [Path]),

    NewState = get_metadata(State#state{dirs=Dirs, file=File, fspath=Path}),

    {noreply, NewState};

handle_cast({line, _, Value, TS}, State) when TS < ?EPOCH_SECONDS_2000 ->
    lager:warning("received value ~w for '~p' for pre year 2000 - skipping value",
                 [Value, State#state.path]),
    {noreply, State};

handle_cast({line, _Path, Value, TS}, State) ->
    NewState = cache_point(Value, TS, State),

    % TODO: more dynamic flush interval
    if NewState#state.cache_size > ?MIN_CACHE_ENTRIES ->
           gen_server:cast(self(), flush_cache);
       true -> ok
    end,

    {noreply, NewState};

handle_cast(flush_cache, State) ->
    case State#state.cache of
        [] -> {noreply, State};
        Cs ->
            lager:debug("flushing metric cache of ~p (~w entries)",
                        [State#state.path, State#state.cache_size]),

            File = State#state.fspath,
            case statser_whisper:update_points(File, Cs) of
                ok ->
                    statser_instrumentation:increment(<<"committed-points">>, State#state.cache_size),
                    {noreply, State#state{cache=[], cache_size=0}};
                _Error ->
                    % archive is not existing (yet)
                    % so dispatch a creation via rate-limiter
                    gen_server:cast(create_limiter, {drain, create, self()}),
                    {noreply, State}
            end
    end;

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
handle_info(create, State) ->
    Metadata = get_or_create_metadata(State#state.fspath, State#state.path),

    gen_server:cast(self(), flush_cache),
    {noreply, State#state{metadata=Metadata}};

handle_info(_Info, State) ->
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
terminate(_Reason, State) ->
    Path = State#state.path,
    ets:delete(metrics, Path),
    lager:info("terminating metric handler of '~p'", [Path]),
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


get_whisper_file(Path) ->
    {ok, Dirs, File} = get_directory(Path),
    prepare_file(Dirs, File).


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_metadata(State) ->
    case statser_whisper:read_metadata(State#state.fspath) of
        {ok, M} ->
            State#state{metadata=M};
        _Error ->
            % archive is not existing (yet)
            % so dispatch a creation via rate-limiter
            gen_server:cast(create_limiter, {drain, create, self()}),

            {As, A, XFF} = get_creation_metadata(State#state.path),
            {ok, M} = statser_whisper:prepare_metadata(As, A, XFF),
            State#state{metadata=M}
    end.


get_or_create_metadata(FsPath, Path) ->
    % now we make sure the file is created if not already
    case statser_whisper:read_metadata(FsPath) of
        {ok, M} -> M;
        {error, enoent} ->
            lager:info("no archive existing for ~p - creating now", [Path]),
            {As, A, XFF} = get_creation_metadata(Path),
            {ok, M} = statser_whisper:create(FsPath, As, A, XFF),
            statser_instrumentation:increment(<<"creates">>),
            M;
        UnexpectedError ->
            lager:warning("failed to read archive - error: ~w", [UnexpectedError]),
            lager:info("no valid archive existing for ~p - creating now", [Path]),
            {As, A, XFF} = get_creation_metadata(Path),
            {ok, M} = statser_whisper:create(FsPath, As, A, XFF),
            statser_instrumentation:increment(<<"creates">>),
            M
    end.


get_directory(Path) ->
    case binary:split(Path, <<".">>, [global, trim_all]) of
        [] -> error;
        Parts ->
            File0 = lists:last(Parts),
            Suffix = <<".wsp">>,
            File = <<File0/binary, Suffix/binary>>,
            Dirs = lists:droplast(Parts),
            {ok, Dirs, File}
    end.


to_file(Parts) ->
    F = fun(A, <<>>) -> <<A/binary>>;
           (A, B) -> <<A/binary, "/", B/binary>>
        end,
    lists:foldr(F, <<>>, Parts).


prepare_file(Dirs, File) ->
    BaseDir = statser_config:get_data_dir(),
    Path = to_file([BaseDir | Dirs ++ [File]]),
    ok = filelib:ensure_dir(Path),
    Path.


get_creation_metadata(Path) ->
    {Storage, Aggregation} = statser_config:get_metadata(Path),
    Retentions = Storage#storage_definition.retentions,

    lager:info("creating new archive with definition: ~p ~p", [Retentions, Aggregation]),

    RetValues = lists:map(fun (#retention_definition{seconds=S, points=P}) -> {S, P} end,
                          Retentions),

    Agg = Aggregation#aggregation_definition.aggregation,
    XFF = Aggregation#aggregation_definition.factor,

    {RetValues, Agg, XFF}.


merge_with_cache(Series, []) ->
    Series;
merge_with_cache(Series, Cache) ->
    Values = merge_with_cache(Series#series.values, lists:reverse(Cache), []),
    Series#series{values=lists:reverse(Values)}.

merge_with_cache([], _Cache, Acc) -> Acc;
merge_with_cache([{TS, null}=P | Ps], Cache, Acc) ->
    % TODO: cope with timestamp alignment
    case advance_cache(TS, Cache) of
        [{TS, _}=C | Cs] -> merge_with_cache(Ps, Cs, [C | Acc]);
        Cs -> merge_with_cache(Ps, Cs, [P | Acc])
    end;

merge_with_cache([P | Ps], Cache, Acc) ->
    merge_with_cache(Ps, Cache, [P | Acc]).


advance_cache(_TS, []) -> [];
advance_cache(TS, [{TS1, _} | Cs]) when TS > TS1 ->
    advance_cache(TS, Cs);
advance_cache(_TS, Cache) -> Cache.


cache_point(Value, TS, State) ->
    case State#state.metadata of
        #whisper_metadata{archives=[A | _]} ->
            AlignedTS = TS - (TS rem A#whisper_archive.seconds),
            {Cache, Added} = cache_sorted(Value, AlignedTS, State#state.cache),
            CacheSize = State#state.cache_size + Added,
            State#state{cache=Cache, cache_size=CacheSize};
        _Otherwise ->
            State
    end.


cache_sorted(Value, TS, []) ->
    {[{TS, Value}], 1};
cache_sorted(Value, TS, Cache) ->
    cache_sorted(Value, TS, Cache, []).

cache_sorted(Value, TS, [], Acc) ->
    {lists:reverse(Acc) ++ [{TS, Value}], 1};
cache_sorted(Value, TS, [{TS1, _}=P | Ps]=Pss, Acc) ->
    if TS == TS1 ->
           {lists:reverse(Acc) ++ [{TS, Value} | Ps], 0};
       TS > TS1 ->
           {lists:reverse(Acc) ++ [{TS, Value} | Pss], 1};
       true ->
           cache_sorted(Value, TS, Ps, [P | Acc])
    end.


%%
%% TESTS
%%

-ifdef(TEST).

get_directory_test_() ->
    [?_assertEqual(error, get_directory(<<>>)),
     ?_assertEqual(error, get_directory(<<"">>)),
     ?_assertEqual({ok, [], <<"foo.wsp">>}, get_directory(<<"foo">>)),
     ?_assertEqual({ok, [<<"foo">>], <<"bar.wsp">>}, get_directory(<<"foo.bar">>)),
     ?_assertEqual({ok, [<<"foo">>, <<"bar">>], <<"test.wsp">>}, get_directory(<<"foo.bar.test">>))
    ].

to_file_test_() ->
    Test = fun(X) ->
                   {ok, Dirs, File} = get_directory(X),
                   to_file(Dirs ++ [File])
           end,

    [?_assertEqual(<<"foo/bar/test.wsp">>, Test(<<"foo.bar.test">>)),
     ?_assertEqual(<<"foo.wsp">>, Test(<<"foo">>))
    ].

cache_sorted_test_() ->
    [?_assertEqual({[{120, 10}, {110, 10}, {100, 10}], 1},
                  cache_sorted(10, 120, [{110, 10}, {100, 10}])),
     ?_assertEqual({[{120, 10}, {110, 10}, {100, 10}], 1},
                  cache_sorted(10, 110, [{120, 10}, {100, 10}])),
     ?_assertEqual({[{120, 10}, {110, 10}, {100, 10}], 1},
                  cache_sorted(10, 100, [{120, 10}, {110, 10}])),
     ?_assertEqual({[{120, 11}, {110, 10}], 0},
                  cache_sorted(11, 120, [{120, 10}, {110, 10}])),
     ?_assertEqual({[{120, 10}, {110, 11}], 0},
                  cache_sorted(11, 110, [{120, 10}, {110, 10}]))
    ].


merge_with_cache_test_() ->
    {Cache, _} = cache_sorted(16, 160, [{120, 12}, {110, 11}, {100, 10}]),
    Test = fun(Values) ->
                   Series = #series{values=Values},
                   merge_with_cache(Series, Cache)
           end,
    [?_assertEqual(#series{values=[{100, 10}, {110, 11}, {120, 12}]},
                   Test([{100, 10}, {110, null}, {120, null}])),

     ?_assertEqual(#series{values=[{100, 10}, {110, 11}, {120, 12}, {130, 13}]},
                   Test([{100, 10}, {110, null}, {120, 12}, {130, 13}])),

     ?_assertEqual(#series{values=[{100, 10}, {110, 11}, {120, 12}, {130, null}]},
                   Test([{100, 10}, {110, null}, {120, 12}, {130, null}])),

     ?_assertEqual(#series{values=[{100, 10}, {110, 11}, {120, 12}, {130, null}, {160, 16}]},
                   Test([{100, 10}, {110, null}, {120, 12}, {130, null}, {160, null}]))
    ].


-endif. % TEST
