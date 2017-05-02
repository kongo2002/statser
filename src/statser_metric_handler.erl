-module(statser_metric_handler).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(EPOCH_SECONDS_2000, 946681200).

-record(state, {path, dirs, file, fspath, metadata, cache=[]}).

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
handle_call({fetch, From, Until}, _From, State) ->
    File = State#state.fspath,
    ReadFromFile = statser_whisper:fetch(File, From, Until),
    Cached = State#state.cache,
    % TODO: merge
    Merged = ReadFromFile ++ Cached,
    {reply, Merged, State};

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

    {noreply, State#state{dirs=Dirs, file=File, fspath=Path}};

handle_cast({line, _, Value, TS}, State) when TS < ?EPOCH_SECONDS_2000 ->
    lager:warning("received value ~w for '~p' for pre year 2000 - skipping value",
                 [Value, State#state.path]),
    {noreply, State};

handle_cast({line, _Path, Value, TS}, State) ->
    File = State#state.fspath,

    case statser_whisper:update_point(File, Value, TS) of
        ok ->
            statser_instrumentation:increment(<<"committed-points">>),
            {noreply, State};
        _Error ->
            % archive is not existing (yet)
            % so dispatch a creation via rate-limiter
            gen_server:cast(create_limiter, {drain, create, self()}),
            NewState = cache_point(Value, TS, State),
            {noreply, NewState}
    end;

handle_cast(flush_cache, State) ->
    case State#state.cache of
        [] -> {noreply, State};
        Cs ->
            lager:info("flushing metric cache of ~p (~w entries)",
                       [State#state.path, length(Cs)]),

            % TODO: in here we could introduce some kind of bulk write
            %       instead of writing every cached point separately
            File = State#state.fspath,
            lists:foreach(fun({V, TS}) ->
                                  ok = statser_whisper:update_point(File, V, TS),
                                  statser_instrumentation:increment(<<"committed-points">>)
                          end, Cs),
            {noreply, State#state{cache=[]}}
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
    % now we make sure the file is created if not already
    Path = State#state.fspath,
    Metadata = case statser_whisper:read_metadata(Path) of
        {ok, M} -> M;
        {error, enoent} ->
            lager:info("no archive existing for ~p - creating now", [State#state.path]),
            {As, A, XFF} = get_creation_metadata(State#state.path),
            {ok, M} = statser_whisper:create(Path, As, A, XFF),
            statser_instrumentation:increment(<<"creates">>),
            M;
        UnexpectedError ->
            lager:warning("failed to read archive - error: ~w", [UnexpectedError]),
            lager:info("no valid archive existing for ~p - creating now", [State#state.path]),
            {As, A, XFF} = get_creation_metadata(State#state.path),
            {ok, M} = statser_whisper:create(Path, As, A, XFF),
            statser_instrumentation:increment(<<"creates">>),
            M
    end,

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    Path = to_file(Dirs ++ [File]),
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


cache_point(Value, TS, State) ->
    % TODO: sort by timestamp?
    Cache = State#state.cache,
    State#state{cache=[{Value, TS} | Cache]}.

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

-endif. % TEST
