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
-export([start_link/1, send/2, send/3, reply/3, is_alive/2, get_config/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-behaviour(gen_server).

-record(state, {module, config, handler, channel_state, buffer = [], pending = #{}}).

-type event() :: deliver | do_nothing | buffer.

-type state() :: #state{module :: module(), config :: map(), channel_state :: channel_state()}.

-define(LOG_INFO(X, Y), logger:info(X, Y)).
-define(LOG_INFO(X), logger:info(X)).

%%%===================================================================
%%% Callback declarations
%%%===================================================================

%% @doc init_channel:
%% Initializes a new channel.
%% Config parameters allow to specify different communication patterns.
-callback init_channel(Config :: channel_config()) ->
  {ok, State :: channel_state()} | {error, Reason :: term()}.

%% Sends a message using the configured channel.
%% Msg: pub_sub_msg or rpc_msg
%% Params: additional parameter for the request. See each module for details.
%%  wait: can be used to wait on a single rpc message
-callback send(Msg :: message(), Params :: map(), State :: channel_state()) ->
  {ok, State :: channel_state()}.


%% Decide what to do with a message received from the underlying channel.
%% Allows buffering, ignoring, or delivery of a message.
%% Buffered messages are stored until the next message to deliver arrives.
-callback deliver_message(Info :: message_payload(), State :: channel_state()) -> {
  event(), message_payload()} | {event(), internal_msg(), message_payload()} | {error, Reason :: atom()}.

%% Delivers an incoming message to the subscribing handler.
-callback handle_message(Msg :: internal_msg(), State :: channel_state()) ->
  {ok, NewState :: channel_state()} | {error, Reason :: atom()}.


%% Utility to check if a remote point is live.
-callback is_alive(NetworkParams :: term()) -> true | false.

%% Called by handler to reply to a rpc request.
-callback reply(RequestId :: reference(), Reply :: any(), State :: channel_state()) -> any().

%-callback subscribe(Topics :: [binary()], State :: channel_state()) ->
%  {ok, NewState :: channel_state()} | {error, Reason :: atom()}.

%%%===================================================================
%%% API
%%%===================================================================

%% Config map accepts the following parameters
%%  module: the backend module to be used
%%  pattern: pub_sub or rpc
%%  async: [for rpc] blocking or asynchronous send
%%  load_balanced: when supported allows load balancing of subscribers/clients
%%  network_params: specific configurations for the chosen module (antidote_channel.hrl)
%%  namespace: [for pub_sub] namespace for messages
%%  topics: [for pub_sub] list of topics for subscriber
%%  handler: the process that will receive messages in the mailbox
-spec start_link(Config :: map()) ->
  {ok, Pid :: pid()} |
  ignore |
  {error, Reason :: atom()}.

start_link(#{module := Mod} = ConfigMap) ->
  case get_config(ConfigMap) of
    {error, _} = E -> E;
    Config -> gen_server:start_link(?MODULE, [Mod, Config, ConfigMap], [])
  end;

start_link(ConfigMap) ->
  {error, {bad_configuration, ConfigMap}}.

-spec send(Pid :: pid(), Msg :: message()) -> ok.

send(Pid, Msg) ->
  gen_server:call(Pid, {send, Msg, #{}}).

-spec send(Pid :: pid(), Msg :: message(), Prams :: map()) -> ok.
send(Pid, Msg, Params) ->
  gen_server:call(Pid, {send, Msg, Params}).

reply(Pid, RequestId, Msg) ->
  gen_server:call(Pid, {reply, RequestId, Msg}).

%-spec subscribe(Pid :: pid(), Topics :: [binary()]) -> ok.

%subscribe(Pid, Topics) ->
%  gen_server:cast(Pid, {add_subscriptions, Topics}).

-spec is_alive(ChannelType :: channel_type(), NetworkParams :: term()) -> true | false.

is_alive(ChannelType, NetworkParams) ->
  ChannelType:is_alive(NetworkParams).

-spec stop(Pid :: pid()) -> ok.

stop(Pid) ->
  gen_server:stop(Pid).

-spec get_config(ConfigMap :: map()) -> channel_config() | {error, Reason :: atom()}.
get_config(#{module := Mod, pattern := Pattern, network_params := NetworkConfig} = Config0) ->
  try
    case Mod:get_network_config(Pattern, NetworkConfig) of
      {error, _} = E -> E;
      Config1 -> get_config(Pattern, Mod, Config0, Config1)
    end
  catch
    Exception -> {error, Exception}
  end.

get_config(pub_sub, Mod, ConfigMap, NetworkConfig) ->
  Default = #pub_sub_channel_config{},
  #pub_sub_channel_config{
    module = Mod,
    topics = maps:get(topics, ConfigMap, Default#pub_sub_channel_config.topics),
    namespace = maps:get(namespace, ConfigMap, Default#pub_sub_channel_config.namespace),
    handler = maps:get(handler, ConfigMap, Default#pub_sub_channel_config.handler),
    network_params = NetworkConfig
  };

get_config(rpc, Mod, ConfigMap, NetworkConfig) ->
  Default = #rpc_channel_config{},
  #rpc_channel_config{
    module = Mod,
    handler = maps:get(handler, ConfigMap, Default#rpc_channel_config.handler),
    load_balanced = maps:get(load_balanced, ConfigMap, Default#rpc_channel_config.load_balanced),
    async = maps:get(async, ConfigMap, Default#rpc_channel_config.async),
    network_params = NetworkConfig
  };

get_config(_Pattern, _Mod, _ConfigMap, _NetworkConfig) ->
  {error, pattern_not_supported}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: term()) ->
  {ok, State :: term()} | {ok, State :: term(), timeout() | hibernate | {continue, term()}} |
  {stop, Reason :: term()} | ignore.

init([Mod, Config, ConfigMap]) ->
  Res = Mod:init_channel(Config),
  case Res of
    {ok, InitState} ->
      {ok, #state{module = Mod, config = ConfigMap, channel_state = InitState}};
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

handle_call({is_alive, Pattern, Attributes}, _, #state{module = Mod} = State) ->
  {reply, Mod:is_alive(Pattern, Attributes), State};

handle_call({send, #rpc_msg{} = Msg, Params}, From, #state{module = Mod, channel_state = S, pending = Pending} = State) ->
  Ref = make_ref(),
  {ok, S1} = Mod:send(Msg#rpc_msg{request_id = Ref}, Params, S),
  case is_sync(Params, State) of
    true ->
      {noreply, State#state{channel_state = S1, pending = Pending#{Ref => From}}};
    _ -> {reply, Ref, State#state{channel_state = S1}}
  end;

handle_call({send, #pub_sub_msg{} = Msg, Params}, _From, #state{module = Mod, channel_state = S} = State) ->
  {ok, S1} = Mod:send(Msg, Params, S),
  {reply, ok, State#state{channel_state = S1}};

handle_call({reply, RequestId, Reply}, _From, #state{module = Mod, channel_state = S} = State) ->
  {Res, S1} = Mod:reply(RequestId, Reply, S),
  {reply, Res, State#state{channel_state = S1}};

handle_call({add_subscriptions, Topics}, _From, #state{module = Mod, channel_state = S} = State) ->
  {ok, S1} = Mod:add_subscriptions(Topics, S),
  {noreply, State#state{channel_state = S1}};

handle_call(Any, _, State) ->
  {reply, {error, {unhandled_message, Any}}, State}.

handle_cast(Info, State) ->
  ?LOG_INFO("Unknown message received ~p", [Info]), {noreply, State}.

-spec handle_info(Info :: timeout | term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate | {continue, term()}} |
  {stop, Reason :: term(), NewState :: state()}.

handle_info(Info, #state{module = Mod, channel_state = S, buffer = Buff} = State) ->
  case Mod:deliver_message(Info, S) of
    {do_nothing, _} -> {noreply, State};
    {buffer, M} -> {noreply, State#state{buffer = [M | Buff]}};
    {deliver, #internal_msg{meta = Meta} = Msg, Original} ->
      Msg1 = Msg#internal_msg{meta = Meta#{buffered => [Original | Buff]}},
      case handle_message(Msg1, State) of
        {ok, State1} -> {noreply, State1#state{buffer = []}};
        {{error, _} = Error, State1} -> {stop, Error, State1#state{buffer = []}}
      end;
    _ ->
      {stop, {error, unmarshalling, Info}}
  end;

handle_info(Info, State) ->
  ?LOG_INFO("Unknown message received ~p", [Info]), {noreply, State}.

-spec handle_message(Msg :: internal_msg(), State :: state()) ->
  {ok, NewState :: state()} | {Error :: {error, Reason :: atom()}, State :: state()}.

handle_message(
    Msg = #internal_msg{
      payload = #rpc_msg{request_id = RId, reply_payload = P}},
    #state{module = Mod, channel_state = S, pending = Pending} = State) when P =/= undefined ->
  case maps:find(RId, Pending) of
    {ok, To} -> gen_server:reply(To, P), {ok, State#state{pending = maps:remove(RId, Pending)}};
    _ -> {R, Si} = Mod:handle_message(Msg, S), {R, State#state{channel_state = Si}}
  end;

handle_message(Msg, #state{module = Mod, channel_state = S} = State) ->
  {Resp, S1} = Mod:handle_message(Msg, S),
  {Resp, State#state{channel_state = S1}}.

-spec terminate(Reason :: atom(), State :: state()) -> Void :: any().

terminate(Reason, #state{module = Mod, channel_state = S} = _State) ->
  Mod:terminate(Reason, S).

-spec code_change(OldVsn :: term(), State :: tuple(), Extra :: term()) ->
  {ok, NewState :: tuple()}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

is_sync(_Params, #state{config = #{async := false}}) ->
  true;

is_sync(Params, _State) ->
  maps:is_key(wait, Params).