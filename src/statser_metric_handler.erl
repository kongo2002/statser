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

-module(statser_metric_handler).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
         start_link/2,
         get_whisper_file/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(EPOCH_SECONDS_2000, 946681200).

-define(MIN_CACHE_INTERVAL, 60).

-define(INACTIVITY_FACTOR, 5).

-record(state, {
          path :: binary(),
          fspath :: binary() | undefined,
          metadata :: #whisper_metadata{} | none,
          cache=[] :: [metric_tuple()],
          heartbeat=0 :: integer(),
          cache_size=0 :: integer()
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
-spec start_link(binary()) -> {ok, _} | ignore | {error, _}.
start_link(Path) ->
    gen_server:start_link(?MODULE, {Path, none}, []).


-spec start_link(binary(), whisper_metadata() | none) -> {ok, _} | ignore | {error, _}.
start_link(Path, Metadata) ->
    gen_server:start_link(?MODULE, {Path, Metadata}, []).


-spec get_whisper_file(binary()) -> binary().
get_whisper_file(Path) ->
    {ok, Dirs, File} = get_directory(Path),
    prepare_file(Dirs, File).

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
init({Path, Metadata}) ->
    lager:debug("initializing metric handler for path '~s' [~w]", [Path, self()]),

    % let's try to register ourselves
    case ets:insert_new(metrics, {Path, self()}) of
        true ->
            % we could register ourself, let's continue with preparation
            gen_server:cast(self(), {prepare, Metadata}),
            {ok, set_heartbeat(#state{path=Path, metadata=none})};
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
            % TODO: in case we could not fetch any values we could
            % at least return the currently cached values instead
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
handle_cast({prepare, Metadata}, State) ->
    Handler = {local, self()},
    statser_finder:register_metric_handler(State#state.path, Handler),

    % first we have to determine the proper path and file to use
    {ok, Dirs, File} = get_directory(State#state.path),

    Path = prepare_file(Dirs, File),
    NewState = get_metadata(State#state{fspath=Path, metadata=Metadata}),

    {noreply, NewState};

handle_cast({line, _, Value, TS}, State) when TS < ?EPOCH_SECONDS_2000 ->
    lager:warning("received value ~w for '~p' for pre year 2000 - skipping value",
                 [Value, State#state.path]),
    {noreply, State};

handle_cast({line, _Path, Value, TS}, State) ->
    NewState = cache_point(Value, TS, State),

    case State#state.metadata of
        #whisper_metadata{archives=[A | _]} ->
            MinInterval = A#whisper_archive.seconds,
            MinCache = ?MIN_CACHE_INTERVAL div MinInterval,

            if NewState#state.cache_size > MinCache ->
                   request_flush();
               true -> ok
            end;
        _Otherwise -> ok
    end,

    {noreply, set_heartbeat(NewState)};

handle_cast(check_heartbeat, #state{path=Path, heartbeat=HB} = State) ->
    case {heartbeat_expired(State), State#state.cache} of
        {true, []} ->
            DateTime = statser_util:epoch_seconds_to_datetime(HB),
            lager:info("stopping metrics handler for '~s' due to inactivity [last seen ~s]", [Path, DateTime]),
            {stop, normal, State};
        {true, _Cs} ->
            % we might have some data points that are not flushed to disk yet
            % let's request a flush and terminate afterwards
            %
            % because usually all metric handlers receive the heartbeat check
            % signal at the same time the chances are real that a couple of
            % handlers will expire at the same time - therefore we don't want
            % to exceed our flush limit by stopping a lot of handlers with
            % pending metrics at once
            request_flush(),
            {noreply, State};
        _Otherwise ->
            {noreply, State}
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
    Metadata = get_or_create_metadata(State#state.fspath, State#state.path, State#state.metadata),

    request_flush(),
    {noreply, State#state{metadata=Metadata}};

handle_info(flush_cache, State) ->
    State0 = flush(State),
    {noreply, State0};

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
    statser_finder:unregister_metric_handler(Path),

    lager:debug("terminating metric handler of '~s'", [Path]),

    flush(State),
    ets:delete(metrics, Path).

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

request_create() ->
    gen_server:cast(create_limiter, {drain, create, self()}).


request_flush() ->
    gen_server:cast(update_limiter, {drain, flush_cache, self()}).


heartbeat_expired(#state{heartbeat=HB, metadata=#whisper_metadata{archives=[A | _]}}) ->
    Now = statser_util:seconds(),
    IntervalMillis = A#whisper_archive.seconds * ?INACTIVITY_FACTOR,
    Expired = Now - IntervalMillis,
    HB < Expired;
heartbeat_expired(_State) -> false.


get_metadata(State) ->
    % if a file already exists we will choose that metadata
    % no matter what is specified in `State#state.metadata`
    case statser_whisper:read_metadata(State#state.fspath) of
        {ok, M} ->
            State#state{metadata=M};
        _Error ->
            % archive is not existing (yet)
            % so dispatch a creation via rate-limiter
            request_create(),

            % we either choose the metadata config already specified
            % in `State#state.metadata` or we determine it based on
            % its path and the storage/aggregation rules
            case State#state.metadata of
                none ->
                    {As, A, XFF} = get_creation_metadata(State#state.path),
                    {ok, M} = statser_whisper:prepare_metadata(As, A, XFF),
                    State#state{metadata=M};
                Else ->
                    lager:debug("~p: using pre-configured metadata: ~p", [State#state.path, Else]),
                    State
            end
    end.


get_or_create_metadata(FsPath, Path, Metadata) ->
    % now we make sure the file is created if not already
    case statser_whisper:read_metadata(FsPath) of
        {ok, M} -> M;
        {error, enoent} ->
            lager:info("no archive existing for '~s' - creating now", [Path]),
            ok = filelib:ensure_dir(FsPath),
            {ok, M} = statser_whisper:create(FsPath, Metadata),
            statser_instrumentation:increment(<<"creates">>),
            M;
        UnexpectedError ->
            lager:warning("failed to read archive - error: ~w", [UnexpectedError]),
            lager:info("no valid archive existing for '~s' - creating now", [Path]),
            ok = filelib:ensure_dir(FsPath),
            {ok, M} = statser_whisper:create(FsPath, Metadata),
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
    to_file([BaseDir | Dirs ++ [File]]).


get_creation_metadata(Path) ->
    {Storage, Aggregation} = statser_config:get_metadata(Path),
    Retentions = Storage#storage_definition.retentions,
    Agg = Aggregation#aggregation_definition.aggregation,
    XFF = Aggregation#aggregation_definition.factor,

    lager:info("~s: determined archive definition [retention: ~p; aggregation: ~p]",
               [Path, lists:map(fun(#retention_definition{raw=R}) -> R end, Retentions), Agg]),

    RetValues = lists:map(fun (#retention_definition{seconds=S, points=P}) -> {S, P} end,
                          Retentions),

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


flush(#state{cache=[]} = State) ->
    State;
flush(#state{cache=Cache, fspath=File} = State) ->
    lager:debug("flushing metric cache of ~p (~w entries)",
                [State#state.path, State#state.cache_size]),

    case statser_whisper:update_points(File, Cache) of
        ok ->
            statser_instrumentation:increment(<<"committed-points">>, State#state.cache_size),
            statser_instrumentation:increment(<<"writes">>),
            State#state{cache=[], cache_size=0};
        _Error ->
            % archive is not existing (yet)
            % so dispatch a creation via rate-limiter
            request_create(),
            State
    end.


set_heartbeat(State) ->
    State#state{heartbeat=statser_util:seconds()}.


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
