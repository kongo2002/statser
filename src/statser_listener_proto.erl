-module(statser_listener_proto).

-behaviour(gen_server).

-include("carbon.hrl").

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

    gen_server:cast(self(), accept),

    {ok, #state{socket=Socket}}.

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

    Filters = statser_config:get_metric_filters(),

    % trigger new listener
    statser_listeners_sup:start_listener(protobuf_listeners),

    self() ! read,

    {noreply, State#state{socket=Socket, filters=Filters}};

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
    Length = read_length(Socket),
    {ok, Data} = gen_tcp:recv(Socket, Length),

    Payload = carbon:decode_msg(Data, 'Payload'),
    lager:debug("read data: ~p", [Payload]),

    lists:foreach(fun(#'Metric'{metric=M, points=Ps}) ->
                          Metric = list_to_binary(M),
                          lists:foreach(fun(#'Point'{value=V, timestamp=TS}) ->
                                                gen_server:cast(statser_router, {line, Metric, V, TS})
                                        end, Ps)
                  end, Payload#'Payload'.metrics),

    self() ! read,
    {noreply, State};

handle_info({tcp_closed, Sock}, State) ->
    lager:debug("socket ~w closed [~w]", [Sock, self()]),
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
    statser_listeners_sup:start_listener(protobuf_listeners),
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
    {ok, <<Length:32/unsigned-integer-big>> = Data} = gen_udp:recv(Socket, ?LENGTH_SIZE),
    lager:debug("read length: ~p (~p)", [Length, Data]),
    Length.
