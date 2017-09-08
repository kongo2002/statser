-module(statser_listener).

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

-record(state, {socket, pattern, filters}).

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
start_link(Socket) ->
    gen_server:start_link(?MODULE, Socket, []).

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
init(Socket) ->
    lager:debug("starting new listener instance [~w]", [self()]),

    Pattern = binary:compile_pattern([<<" ">>, <<"\t">>]),
    Filters = statser_config:get_metric_filters(),

    gen_server:cast(self(), accept),

    {ok, #state{socket=Socket, pattern=Pattern, filters=Filters}}.

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
handle_cast(accept, State) ->
    % accept connection
    {ok, Socket} = gen_tcp:accept(State#state.socket),

    listen(Socket),

    % trigger new listener
    statser_listeners_sup:start_listener(),

    {noreply, State#state{socket=Socket}};

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
handle_info({tcp, _Sock, Data}, State) ->
    case process_line(State, Data) of
        {skip, _, _} ->
            statser_instrumentation:increment(<<"metrics-blacklisted">>);
        {Path, {ok, Value}, {ok, TimeStamp}} ->
            Line = {line, Path, Value, TimeStamp},
            gen_server:cast(statser_router, Line),

            statser_instrumentation:increment(<<"metrics-received">>),
            lager:debug("received ~p: ~w at ~w", [Path, Value, TimeStamp]);
        _Error ->
            statser_instrumentation:increment(<<"invalid-metrics">>),
            lager:warning("invalid metric received: ~p", [Data])
    end,

    listen(State#state.socket),
    {noreply, State};

handle_info({tcp_closed, Sock}, State) ->
    lager:debug("socket ~w closed [~w]", [Sock, self()]),

    statser_listeners_sup:start_listener(),
    {stop, normal, State};

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
terminate(_Reason, _State) ->
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

listen(Socket) ->
    inet:setopts(Socket, [{active, once}]).

process_line(#state{pattern=Pattern, filters=Filters}, Data) ->
    case binary:split(Data, Pattern, [global, trim_all]) of
        % usual graphite format: 'path value timestamp'
        [Path, ValueBS, TimeStampBS] ->
            Value = statser_util:to_number(ValueBS),
            TimeStamp = to_epoch(TimeStampBS),
            {filter_path(Path, Filters), Value, TimeStamp};

        % graphite format w/o timestamp: 'path value'
        [Path, ValueBS] ->
            Value = statser_util:to_number(ValueBS),
            TimeStamp = erlang:system_time(second),
            {filter_path(Path, Filters), Value, {ok, TimeStamp}};

        _Otherwise ->
            error
    end.

filter_path(Path, Filters) ->
    case statser_config:metric_passes_filters(Path, Filters) of
        true -> Path;
        false -> skip
    end.

to_epoch(Binary) ->
    List = binary_to_list(Binary),
    case string:to_integer(List) of
        {error, _} -> error;
        {Result, _Rest} -> {ok, Result}
    end.

