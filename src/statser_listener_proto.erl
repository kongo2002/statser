-module(statser_listener_proto).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("carbon.hrl").
-include("statser.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {socket, filters}).

-define(LENGTH_SIZE, 4).

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
    lager:debug("starting new protobuf listener instance [~w]", [self()]),

    gen_server:cast(self(), {accept, Socket}),

    {ok, #state{socket=none}}.

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
handle_cast({accept, Socket}, State) ->
    % accept connection
    {ok, LSocket} = gen_tcp:accept(Socket),

    Filters = statser_config:get_metric_filters(),

    % trigger new listener
    statser_listeners_parent:start_listener(protobuf_listeners),

    self() ! read,

    {noreply, State#state{socket=LSocket, filters=Filters}};

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
handle_info(read, #state{socket=Socket} = State) ->
    case read_length(Socket) of
        {ok, Length} ->
            {ok, Data} = gen_tcp:recv(Socket, Length),

            Payload = carbon:decode_msg(Data, 'Payload'),
            lager:debug("read protobuf data: ~p", [Payload]),

            Metrics = protobuf_to_metrics(Payload, State#state.filters),

            lists:foreach(fun({M, V, TS}) ->
                                  statser_router:line(M, V, TS),
                                  statser_instrumentation:increment(<<"protobuf.handled-values">>)
                          end, Metrics),

            self() ! read,
            {noreply, State};
        error ->
            {stop, normal, State}
    end;

handle_info({tcp_closed, Sock}, State) ->
    lager:info("socket ~w closed [~w]", [Sock, self()]),
    {stop, normal, State};

handle_info(Info, State) ->
    lager:warning("expected message in protobuf listener: ~p", [Info]),
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
    case State#state.socket of
        none ->
            statser_listeners_parent:start_listener(protobuf_listeners);
        Socket ->
            gen_tcp:close(Socket)
    end,
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

read_length(Socket) ->
    case gen_tcp:recv(Socket, ?LENGTH_SIZE) of
        {ok, <<Length:32/unsigned-integer-big>> = Data} ->
            lager:debug("read length: ~p (~p)", [Length, Data]),
            {ok, Length};
        {error, closed} ->
            error;
        Unexpected ->
            lager:error("unexpected error in protobuf listener: ~p", [Unexpected]),
            error
    end.


protobuf_to_metrics(#'Payload'{metrics=Metrics}, Filters) ->
    lists:flatmap(fun(#'Metric'{metric=M, points=Ps}) ->
                          Metric = list_to_binary(M),
                          case statser_config:metric_passes_filters(Metric, Filters) of
                              true ->
                                  lists:map(fun(#'Point'{value=V, timestamp=TS}) ->
                                                    {Metric, V, TS}
                                            end, Ps);
                              false ->
                                  statser_instrumentation:increment(<<"metrics-blacklisted">>),
                                  []
                          end
                  end, Metrics).

%%
%% TESTS
%%

-ifdef(TEST).

metric_to_payload(Metric, Points) ->
    MS = [#'Metric'{metric=Metric, points=Points}],
    #'Payload'{metrics=MS}.


pt(Value, TS) ->
    #'Point'{value=Value, timestamp=TS}.


mk_pattern(Expr) ->
    {ok, Pattern} = re:compile(Expr, [no_auto_capture]),
    #metric_pattern{name=Expr, pattern=Pattern}.


protobuf_to_metrics_test_() ->
    MS1 = metric_to_payload("foo.bar", []),
    MS2 = metric_to_payload("foo.bar", [pt(100, 10), pt(200, 20), pt(300, 30)]),
    MS3 = metric_to_payload("ham.eggs", [pt(100, 10), pt(200, 20), pt(300, 30)]),

    Filters = #metric_filters{blacklist=[mk_pattern("^foo")]},

    [?_assertEqual([], protobuf_to_metrics(MS1, none)),
     ?_assertEqual([{<<"foo.bar">>, 100, 10},
                    {<<"foo.bar">>, 200, 20},
                    {<<"foo.bar">>, 300, 30}],
                   protobuf_to_metrics(MS2, none)),
     ?_assertEqual([], protobuf_to_metrics(MS2, Filters)),
     ?_assertEqual(3, length(protobuf_to_metrics(MS3, Filters)))].

-endif.
