-module(statser_finder_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

%% API
-export([start_link/0,
         find_metrics_files/1,
         find_metrics_files/2,
         get_suffix/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(FINDER_UPDATE_INTERVAL, 60000).

-record(state, {metrics=[], data_dir, timer, count}).

-record(metric_file, {
          name :: nonempty_string(),
          file :: nonempty_string()
         }).

-record(metric_dir, {
          name :: nonempty_string(),
          contents :: []
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
    State0 = update_metrics_files(State),
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
    State0 = update_metrics_files(State),
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

update_metrics_files(#state{data_dir=Dir, count=OldCount}=State) ->
    lager:debug("updating finder metrics files"),

    State0 = case find_metrics_files(Dir) of
                 [#metric_dir{contents=Ms}] ->
                     Count = count_metrics(Ms),

                     if OldCount /= Count ->
                            lager:info("found ~p metrics in data directory '~s'", [Count, Dir]);
                        true -> ok
                     end,

                     State#state{metrics=Ms, count=Count};
                 _Otherwise ->
                     if OldCount /= 0 ->
                            lager:info("no metrics found in data directory '~s' ~p", [Dir]);
                        true -> ok
                     end,
                     State#state{metrics=[], count=0}
             end,

    Timer = erlang:send_after(?FINDER_UPDATE_INTERVAL, self(), update_metrics),
    State0#state{timer=Timer}.


count_metrics(Metrics) ->
    count_metrics(Metrics, 0).

count_metrics([], Count) -> Count;
count_metrics([#metric_file{} | Ms], Count) ->
    count_metrics(Ms, Count+1);
count_metrics([#metric_dir{contents=Cs} | Ms], Count) ->
    count_metrics(Ms, Count + count_metrics(Cs)).


find_metrics_files(File) ->
    find_metrics_files(File, ".").

find_metrics_files(File, Base) ->
    Path = filename:join(Base, File),
    case file:read_file_info(Path, [raw]) of
        {ok, {file_info, _, directory, _, _, _, _, _, _, _, _, _, _, _}} ->
            case file:list_dir(Path) of
                {ok, Fs} ->
                    case lists:flatmap(fun(F) -> find_metrics_files(F, Path) end, Fs) of
                        [] -> [];
                        Contents -> [#metric_dir{name=File, contents=Contents}]
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
    {lists:reverse([]), string:lowercase(Suffix)};
get_suffix(File, 0, Suffix) ->
    {lists:reverse(File), string:lowercase(Suffix)};
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
