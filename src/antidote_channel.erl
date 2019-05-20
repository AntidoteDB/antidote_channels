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

-module(antidote_channel).
-include_lib("antidote_channel.hrl").

%% API
-export([start_link/2, publish_async/3, handle_subscription/2, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-behaviour(gen_server).

-record(state, {module, config, channel, channel_state}).

-type event() :: push_notification | do_nothing.

-type state() :: #state{module :: module(), config :: channel_config(), channel :: channel(), channel_state :: channel_state()}.

%%%===================================================================
%%% Callback declarations
%%%===================================================================

-callback init_channel(Config :: channel_config()) -> {ok, State :: channel_state()} | {error, Reason :: term()}.

-callback publish_async(Topic :: binary(), Msg :: message(), State :: channel_state()) -> {ok, State :: channel_state()}.

-callback handle_subscription(Msg :: message(), State :: channel_state()) -> {ok, NewState :: channel_state()} | {error, Reason :: atom()}.

-callback event_for_message(Info :: term()) -> {ok, event()} | {error, Reason :: atom()}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(Module :: atom(), Config :: channel_config()) ->
  {ok, Pid :: pid()} |
  ignore |
  {error, Reason :: atom()}.

start_link(Mod, Config) ->
  gen_server:start_link(?MODULE, [Mod, Config], []).

-spec publish_async(Pid :: pid(), Topic :: binary(), Msg :: string()) -> ok.

publish_async(Pid, Topic, Msg) ->
  gen_server:cast(Pid, {publish_async, Topic, Msg}).

-spec handle_subscription(Pid :: pid(), Msg :: string()) -> ok.

handle_subscription(Pid, Msg) ->
  gen_server:call(Pid, {handle_subscription, Msg}).

-spec stop(Pid :: pid()) -> ok.

stop(Pid) ->
  gen_server:stop(Pid).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: term()) ->
  {ok, State :: term()} | {ok, State :: term(), timeout() | hibernate | {continue, term()}} |
  {stop, Reason :: term()} | ignore.

init([Mod, Config]) ->
  Res = (catch Mod:init_channel(Config)),
  case Res of
    {ok, InitState} ->
      {ok, #state{module = Mod, config = Config, channel_state = InitState}};
    {error, Reason} -> {stop, Reason}
  end.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
  {reply, Reply :: term(), NewState :: term()} |
  {reply, Reply :: term(), NewState :: term(), timeout() | hibernate | {continue, term()}} |
  {noreply, NewState :: term()} |
  {noreply, NewState :: term(), timeout() | hibernate | {continue, term()}} |
  {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
  {stop, Reason :: term(), NewState :: term()}.

handle_call(_, _, State) -> {noreply, State}.

-spec handle_cast(Request :: term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate | {continue, term()}} |
  {stop, Reason :: term(), NewState :: state()}.

handle_cast({publish_async, Topic, Msg}, #state{module = Mod, channel_state = S} = State) ->
  {ok, S1} = Mod:publish_async(Topic, Msg, S),
  {noreply, State#state{channel_state = S1}}.

-spec handle_info(Info :: timeout | term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate | {continue, term()}} |
  {stop, Reason :: term(), NewState :: state()}.

handle_info(Info, #state{module = Mod} = State) ->
  State2 =
    case Mod:event_for_message(Info) of
      {ok, do_nothing} -> State;
      {ok, Event} ->
        {ok, State1} = handle_message(Event, Info, State), State1;
      _ ->
        logger:info("Unknown message received ~p", [Info]), State
    end,
  {noreply, State2}.

-spec handle_message(Event :: event(), Msg :: message(), State :: state()) ->
  {ok, NewState :: state()}.

handle_message(push_notification, Msg, #state{module = Mod, channel_state = S} = State) ->
  {_Resp, S1} = Mod:handle_subscription(#message{payload = Msg}, S),
  {ok, State#state{channel_state = S1}}.

-spec terminate(Reason :: atom(), State :: state()) -> Void :: any().

terminate(Reason, #state{module = Mod, channel_state = S} = _State) ->
  Mod:terminate(Reason, S).

-spec code_change(OldVsn :: term(), State :: tuple(), Extra :: term()) ->
  {ok, NewState :: tuple()}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
