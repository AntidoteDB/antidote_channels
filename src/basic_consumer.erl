%%%-------------------------------------------------------------------
%%% @author vbalegas
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. May 2019 18:12
%%%-------------------------------------------------------------------
-module(basic_consumer).
-author("vbalegas").

-behaviour(gen_server).

%% API
-export([start/0, start_link/0, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {channel, msg_buffer = [] :: list()}).

start() ->
  gen_server:start(?MODULE, [], []).

start_link() ->
  gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
  gen_server:call(Pid, terminate).

init([]) -> {ok, #state{}}.

%%TODO: document the accepted replies
handle_call(Msg, _From, #state{} = State) ->
  {stop, {unhandled_message, Msg}, ok, State}.

handle_cast({chan_started, #{channel := Channel}}, #state{} = State) ->
  {noreply, State#state{channel = Channel}};

handle_cast({chan_closed, #{reason := Reason}}, #state{} = State) ->
  {stop, Reason, State};

handle_cast(Msg, #state{msg_buffer = Buff} = State) ->
  Buff1 = [Msg | Buff],
  {noreply, State#state{msg_buffer = Buff1}}.

handle_info(Msg, #state{msg_buffer = Buff} = State) ->
  Buff1 = [Msg | Buff],
  {noreply, State#state{msg_buffer = Buff1}};

handle_info(_Msg, State) -> {noreply, State}.
