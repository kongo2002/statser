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

-record(state, {socket, pattern, router}).

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
    Pattern = binary:compile_pattern([<<" ">>, <<"\t">>]),

    gen_server:cast(self(), accept),

    {ok, #state{socket=Socket, pattern=Pattern}}.

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

    % start router instance for this connection
    {ok, Router} = gen_server:start_link(statser_router, [], []),

    % trigger new listener
    statser_sup:start_listener(),

    {noreply, State#state{socket=Socket, router=Router}};

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
    case process_line(State#state.pattern, Data) of
        error ->
            ok;
        Line ->
            State#state.router ! Line
    end,

    listen(State#state.socket),
    {noreply, State};

handle_info({tcp_closed, Sock}, State) ->
    lager:debug("socket ~w closed [~w]~n", [Sock, self()]),

    statser_sup:start_listener(),
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

process_line(Pattern, Data) ->
    case binary:split(Data, Pattern, [global, trim_all]) of
        % usual graphite format: 'path value timestamp'
        [Path, ValueBS, TimeStampBS] ->
            {ok, Value} = to_number(ValueBS),
            {ok, TimeStamp} = to_epoch(TimeStampBS),
            lager:debug("received ~w: ~w at ~w", [Path, Value, TimeStamp]),
            {line, Path, Value, TimeStamp};

        % graphite format w/o timestamp: 'path value'
        [Path, ValueBS] ->
            {ok, Value} = to_number(ValueBS),
            TimeStamp = erlang:system_time(second),
            lager:debug("received ~w: ~w at ~w (generated)", [Path, Value, TimeStamp]),
            {line, Path, Value, TimeStamp};

        _Otherwise ->
            lager:warn("invalid input received: ~w", [Data]),
            error
    end.

to_epoch(Binary) ->
    List = binary_to_list(Binary),
    case string:to_integer(List) of
        {error, _} -> error;
        {Result, _Rest} -> {ok, Result}
    end.

to_number(Binary) ->
    List = binary_to_list(Binary),
    case string:to_float(List) of
        {error, no_float} ->
            case string:to_integer(List) of
                {error, _} -> error;
                {Result, _} -> {ok, Result}
            end;
        {Result, _} -> {ok, Result}
    end.
