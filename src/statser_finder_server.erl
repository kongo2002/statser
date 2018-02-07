-module(statser_finder_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

-define(FINDER_UPDATE_INTERVAL, 60000).

-record(state, {metrics=[], status=none, data_dir, timer, count}).

-record(metric_file, {
          name :: nonempty_string(),
          file :: nonempty_string(),
          handler=undefined :: pid() | undefined
         }).

-record(metric_dir, {
          name :: nonempty_string(),
          metrics,
          dirs
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

update_metrics_files(Dir, OldCount) ->
    Start = erlang:monotonic_time(millisecond),

    case find_metrics_files(Dir) of
        [#metric_dir{dirs=Ms}] ->
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
    count_metrics(Metrics, 0).

count_metrics(Dict, Count) ->
    Sum = fun(_K, #metric_dir{dirs=Dirs, metrics=Metrics}, Acc) ->
                  Acc + orddict:size(Metrics) + count_metrics(Dirs)
          end,
    orddict:fold(Sum, Count, Dict).


find_metrics_files(File) ->
    find_metrics_files(File, ".").

find_metrics_files([$. | _], _Base) -> [];
find_metrics_files(File, Base) ->
    Path = filename:join(Base, File),
    case file:read_file_info(Path, [raw]) of
        {ok, {file_info, _, directory, _, _, _, _, _, _, _, _, _, _, _}} ->
            case file:list_dir(Path) of
                {ok, Contents} ->
                    F = fun(Name, {Fs, Ds}=Acc) ->
                                case find_metrics_files(Name, Path) of
                                    [] -> Acc;
                                    [#metric_file{}=X] -> {[{Name, X} | Fs], Ds};
                                    [#metric_dir{}=X] -> {Fs, [{Name, X} | Ds]}
                                end
                        end,
                    case lists:foldl(F, {[], []}, Contents) of
                        {[], []} -> [];
                        {Metrics, Dirs} ->
                            [#metric_dir{name=File,
                                         metrics=orddict:from_list(Metrics),
                                         dirs=orddict:from_list(Dirs)}]
                    end;
                _Otherwise -> []
            end;
        {ok, {file_info, _, regular, read_write, _, _, _, _, _, _, _, _, _, _}} ->
            case get_suffix(File) of
                {Name, ".wsp"} -> [#metric_file{name=Name, file=File}];
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

-endif.
