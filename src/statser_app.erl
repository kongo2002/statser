%%%-------------------------------------------------------------------
%% @doc statser public API
%% @end
%%%-------------------------------------------------------------------

-module(statser_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, server_loop/2]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    NumListeners = 5,
    Port = 3000,

    ok = case gen_tcp:listen(Port, [{active, false}, binary, {packet, line}]) of
        {ok, ListenSock} ->
            Pattern = binary:compile_pattern([<<" ">>, <<"\t">>]),
            start_servers(Pattern, NumListeners, ListenSock);
        Error -> Error
    end,

    statser_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

start_servers(_Pattern, 0, _) ->
    ok;

start_servers(Pattern, Num, Listen) ->
    spawn(?MODULE, server_loop, [Pattern, Listen]),
    start_servers(Pattern, Num - 1, Listen).

server_loop(Pattern, Port) ->
    % accept loop
    case gen_tcp:accept(Port) of
        {ok, Sock} ->
            io:format("starting server loop at ~w~n", [self()]),
            loop(Pattern, Sock),
            server_loop(Pattern, Port);
        Other ->
            io:format("accept returned ~w - goodbye!~n",[Other]),
            ok
    end.

loop(Pattern, Sock) ->
    inet:setopts(Sock,[{active, once}]),
    receive
        {tcp, Sock, Data} ->
            ok = process_line(Pattern, Data),
            loop(Pattern, Sock);
        {tcp_closed, Sock} ->
            io:format("Socket ~w closed [~w]~n", [Sock, self()]),
            ok
    end.

process_line(Pattern, Data) ->
    case binary:split(Data, Pattern, [global, trim_all]) of
        [Path, ValueBS, TimeStampBS] ->
            {ok, Value} = to_number(ValueBS),
            {ok, TimeStamp} = to_epoch(TimeStampBS),
            io:format("received ~w: ~w at ~w~n", [Path, Value, TimeStamp]);
        _Otherwise ->
            io:format("invalid input received: ~w~n", [Data])
    end,
    ok.

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
