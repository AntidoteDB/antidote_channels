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
-export([start_link/0, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {msg_buffer = [] :: list()}).

start_link() ->
  gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
  gen_server:call(Pid, terminate).

init([]) -> {ok, #state{}}.

%%TODO: document the accepted replies
handle_call(Msg, _From, #state{msg_buffer = Buff} = State) ->
  Buff1 = [Msg | Buff],
  {reply, ok, State#state{msg_buffer = Buff1}}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Msg, State) -> {noreply, State}.
