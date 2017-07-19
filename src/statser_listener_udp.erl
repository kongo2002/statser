-module(statser_listener_udp).

-behaviour(gen_server).

%% API
-export([start_link/1,
         listen/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {port, socket}).

-define(DELIMITER_PIPE, <<"|">>).
-define(DELIMITER_COLON, <<":">>).

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
start_link(Port) ->
    gen_server:start_link(?MODULE, Port, []).

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
init(Port) ->
    lager:info("starting UDP listener at port ~w", [Port]),

    gen_server:cast(self(), accept),
    {ok, #state{port=Port}}.

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
handle_cast(accept, #state{port=Port} = State) ->
    Self = self(),
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    lager:info("UDP listener opened port at ~w [~p]", [Port, Socket]),

    % TODO: handle exit/failure
    spawn_link(?MODULE, listen, [Self, Socket]),
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
handle_info({msg, Binary}, State) ->
    Parts = binary:split(Binary, <<"\n">>, [global, trim_all]),
    lists:foreach(fun handle/1, Parts),

    {noreply, State};

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

listen(Parent, Socket) ->
    {ok, {_Addr, _Port, Packet}} = gen_udp:recv(Socket, 0),
    Parent ! {msg, Packet},
    listen(Parent, Socket).


handle(Packet) ->
    lager:debug("handle UDP packet: ~p", [Packet]),

    Result = case binary:split(Packet, ?DELIMITER_COLON) of
                 [Metric, Rest] ->
                     Split = binary:split(Rest, ?DELIMITER_PIPE, [global, trim_all]),
                     handle_metric(Metric, Split);
                 _ ->
                     % TODO: check for valid metrics name
                     handle_metric(Packet)
             end,
    case Result of
        ok -> ok;
        error ->
            % TODO: inc error counter
            ok
    end.


handle_metric(Metric) ->
    handle_metric(Metric, {ok, 1}, <<"c">>, 1).

handle_metric(Metric, [Value]) ->
    handle_metric(Metric, statser_util:to_number(Value), <<"c">>, 1);
handle_metric(Metric, [Value, Type]) ->
    handle_metric(Metric, statser_util:to_number(Value), Type, 1);
handle_metric(Metric, [Value, Type, <<"@", Sampling/binary>>]) ->
    {Sample, _Rst} = string:to_float(binary:bin_to_list(Sampling)),
    handle_metric(Metric, statser_util:to_number(Value), Type, Sample);
handle_metric(Metric, Invalid) ->
    lager:debug("invalid metric received: ~p - ~p", [Metric, Invalid]),
    error.

handle_metric(_Metric, error, _Type, _Sample) ->
    lager:debug("invalid value specified"),
    error;

handle_metric(_Metric, _Value, _Type, error) ->
    lager:debug("invalid sample specified"),
    error;

handle_metric(Metric, {ok, Value}, <<"c">>, Sample) ->
    lager:debug("handle counter: ~p:~w [sample ~w]", [Metric, Value, Sample]),
    ok;

handle_metric(Metric, {ok, Value}, <<"ms">>, Sample) ->
    lager:debug("handle timer: ~p:~w [sample ~w]", [Metric, Value, Sample]),
    ok;

handle_metric(Metric, {ok, Value}, <<"g">>, _Sample) ->
    lager:debug("handle gauge: ~p:~w", [Metric, Value]),
    ok;

handle_metric(Metric, {ok, Value}, <<"s">>, _Sample) ->
    lager:debug("handle set: ~p:~w", [Metric, Value]),
    ok;

handle_metric(_Metric, _Value, Type, _Sample) ->
    lager:debug("invalid metric type specified: ~p [supported: c, ms, g, s]", [Type]),
    error.

