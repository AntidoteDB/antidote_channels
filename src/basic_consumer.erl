%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------
-module(basic_consumer).
-author("vbalegas").

-include("antidote_channel.hrl").

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

%handle_cast({chan_started, #{channel := Channel}}, #state{} = State) ->
%  {noreply, State#state{channel = Channel}};

%handle_cast({chan_closed, #{reason := Reason}}, #state{} = State) ->
%  {stop, Reason, State};

handle_info(#rpc_msg{reply_payload = Msg}, #state{msg_buffer = Buff} = State) ->
  Buff1 = [Msg | Buff],
  {noreply, State#state{msg_buffer = Buff1}};

handle_info(#pub_sub_msg{payload = Msg}, #state{msg_buffer = Buff} = State) ->
  Buff1 = [Msg | Buff],
  {noreply, State#state{msg_buffer = Buff1}};

handle_info(_Msg, State) ->
  {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.
