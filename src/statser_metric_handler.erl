-module(statser_metric_handler).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(EPOCH_SECONDS_2000, 946681200).

-record(state, {path, dirs, file, fspath, metadata}).

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
init([Path]) ->
    lager:info("initializing metric handler for path ~p [~w]", [Path, self()]),

    % let's try to register ourselves
    case ets:insert_new(metrics, {Path, self()}) of
        true ->
            % we could register ourself, let's continue
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

    % now we make sure the file is created if not already
    Metadata = case statser_whisper:read_metadata(Path) of
        {ok, M} -> M;
        {error, enoent} ->
            % TODO: rate limit archive creation
            lager:info("no archive existing for ~p - creating now", [State#state.path]),
            Config = load_initial_metadata(State#state.path),
            statser_whisper:create(Path, Config);
        UnexpectedError ->
            lager:warning("failed to read archive - error: ~w", [UnexpectedError]),
            % TODO: rate limit archive creation
            lager:info("no valid archive existing for ~p - creating now", [State#state.path]),
            Config = load_initial_metadata(State#state.path),
            statser_whisper:create(Path, Config)
    end,

    {noreply, State#state{dirs=Dirs, file=File, fspath=Path, metadata=Metadata}};

handle_cast({line, _, Value, TS}, State) when TS < ?EPOCH_SECONDS_2000 ->
    lager:warning("received value ~w for '~p' for pre year 2000 - skipping value",
                 [Value, State#state.path]),
    {noreply, State};

handle_cast({line, _, Value, TS} = Line, State) ->
    File = State#state.fspath,

    lager:debug("got line: ~w", [Line]),

    case statser_whisper:update_point(File, Value, TS) of
        {error, enoent} ->
            % TODO: rate limit archive creation
            lager:info("no archive existing for ~p - creating now", [State#state.path]),

            {ok, _M} = statser_whisper:create(File, State#state.metadata),
            ok = statser_whisper:update_point(File, Value, TS);
        ok -> ok
    end,

    {noreply, State};

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


load_initial_metadata(_Path) ->
    % TODO: read configuration regarding 'archives', 'aggregation' and 'xff'
    Archives = [{10, 360}, {60, 1440}],
    Aggregation = average,
    XFF = 0.5,
    #metadata{archives=Archives, aggregation=Aggregation, xff=XFF}.


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
