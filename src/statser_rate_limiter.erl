% Copyright 2017-2018 Gregor Uhlenheuer
%
% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
% You may obtain a copy of the License at
%
%     http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.

-module(statser_rate_limiter).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("statser.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {limit, remaining, interval, timer, name, pending, members, len}).

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
start_link(Type, Name, Limit) ->
    gen_server:start_link({local, Type}, ?MODULE, [Type, Name, Limit], []).

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
init([Type, Name, LimitPerSec]) ->
    lager:info("starting rate limiter [~w] with limit ~w/sec", [Type, LimitPerSec]),

    % we want to distribute the `refill` as evenly as possible
    % that's why we calculate the rate limit to a minimum interval of 100 ms
    IntervalInMillis = max(?MILLIS_PER_SEC div LimitPerSec, 100),
    LimitPerInterval = LimitPerSec div (?MILLIS_PER_SEC div IntervalInMillis),

    State = #state{limit=LimitPerInterval,
                   remaining=LimitPerInterval,
                   interval=IntervalInMillis,
                   name=Name,
                   pending=queue:new(),
                   members=sets:new(),
                   len=0},

    alive(Name),
    {ok, schedule_refill(State)}.

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
handle_call(drain, _From, State) ->
    case drain(State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        none ->
            {reply, none, State}
    end;

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
handle_cast({drain, Reply, To}, State) ->
    NewState = drain(State, Reply, To),
    {noreply, NewState};

handle_cast(_Cast, State) ->
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
handle_info(refill, #state{limit=Limit, name=Name} = State) ->
    statser_instrumentation:record(<<Name/binary, "-queue-size">>, State#state.len),

    Members = State#state.members,
    {Pending, Members0, Remaining} = drain_queue(State#state.pending, Members, Limit),
    Drained = Limit - Remaining,
    NewLen = State#state.len - Drained,

    NewState = State#state{remaining=Remaining,
                           pending=Pending,
                           members=Members0,
                           len=NewLen},

    {noreply, schedule_refill(NewState)};

handle_info({drain, Reply, To}, State) ->
    NewState = drain(State, Reply, To),
    {noreply, NewState};

handle_info(health, #state{name=Name} = State) ->
    alive(Name),
    {noreply, State};

handle_info(Info, State) ->
    lager:warning("rate-limiter: unhandled message ~p", [Info]),
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
    lager:info("terminating rate limiter at ~w", [self()]),
    erlang:cancel_timer(State#state.timer, [{async, true}]),
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
alive(Name) ->
    statser_health:alive(<<Name/binary, "-rate-limiter">>).


schedule_refill(#state{interval=Interval} = State) ->
    Timer = erlang:send_after(Interval, self(), refill),
    State#state{timer=Timer}.


drain(#state{remaining=Rem} = State) when Rem > 0 ->
    {ok, State#state{remaining=Rem-1}};

drain(#state{name=Name}) ->
    dropped(Name),
    none.


drain(#state{remaining=Rem} = State, Reply, To) when Rem > 0 ->
    To ! Reply,
    State#state{remaining=Rem-1};


drain(#state{name=Name, members=Members} = State, Reply, To) ->
    case sets:is_element(To, Members) of
        false ->
            dropped(Name),

            Item = {Reply, To},
            Pending = queue:in(Item, State#state.pending),
            NewLen = State#state.len + 1,
            Members0 = sets:add_element(To, Members),
            State#state{pending=Pending, members=Members0, len=NewLen};
        true ->
            State
    end.


dropped(Name) ->
    statser_instrumentation:increment(<<Name/binary, "-dropped">>).


drain_queue(Queue, Members, 0) -> {Queue, Members, 0};
drain_queue(Queue, Members, Remaining) ->
    case queue:out(Queue) of
        {empty, _Q} ->
            {Queue, Members, Remaining};
        {{value, {Reply, To}}, Q} ->
            To ! Reply,
            Members0 = sets:del_element(To, Members),
            drain_queue(Q, Members0, Remaining-1)
    end.

%%
%% TESTS
%%

-ifdef(TEST).

setup() ->
    process_flag(trap_exit, true),
    {ok, Pid} = gen_server:start_link(?MODULE, [test, <<"test">>, 2], []),
    Pid.


cleanup(Pid) ->
    exit(Pid, kill),
    ?assertEqual(false, is_process_alive(Pid)).


rate_limiter_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun drain_possible/1,
      fun drain_exceeded/1,
      fun drain_callback_info/1,
      fun drain_callback_cast/1
     ]}.


drain_possible(Pid) ->
    fun() ->
            % limiter is configured with 2/sec
            ?assertEqual(ok, gen_server:call(Pid, drain)),
            timer:sleep(600),
            ?assertEqual(ok, gen_server:call(Pid, drain))
    end.


drain_exceeded(Pid) ->
    fun() ->
            % limiter is configured with 2/sec
            ?assertEqual(ok, gen_server:call(Pid, drain)),
            timer:sleep(600),
            ?assertEqual(ok, gen_server:call(Pid, drain)),
            % this one should fail
            ?assertEqual(none, gen_server:call(Pid, drain))
    end.


drain_callback_info(Pid) ->
    ReceiveWithin = fun(Timeout, Cmd) ->
                            Pid ! {drain, Cmd, self()},
                            receive
                                Resp when Resp == Cmd ->
                                    Cmd
                            after Timeout ->
                                      none
                            end
                    end,
    fun() ->
            % basically immediate response expected
            ?assertEqual(foo, ReceiveWithin(100, foo)),
            % at least 500 ms
            ?assertEqual(foo, ReceiveWithin(600, foo)),
            % shouldn't get a response that fast
            ?assertEqual(none, ReceiveWithin(100, foo))
    end.

drain_callback_cast(Pid) ->
    fun() ->
            Self = self(),
            ExpectCmd = fun() ->
                                gen_server:cast(Pid, {drain, foo, self()}),
                                receive
                                    Resp when Resp == foo ->
                                        Self ! ok
                                after 1600 ->
                                          none
                                end
                        end,

            [spawn_link(ExpectCmd) || _P <- lists:seq(1, 3)],
            Results = [receive ok -> ok after 1600 -> none end || _P <- lists:seq(1, 3)],

            ?assertEqual([ok, ok, ok], Results)
    end.
-endif.
