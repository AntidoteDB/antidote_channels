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
-module(channel_rabbitmq).
-include_lib("antidote_channel.hrl").
-include_lib("common_test/include/ct.hrl").

-behavior(antidote_channel).

%subscriber must be a gen_server. We can put proxy instead, to abstract the handler.
-record(channel_state, {
  config,
  channel,
  connection,
  exchange,
  handler,
  subscriber_tags,
  async,
  auto_ack,
  pending = #{},
  rpc_queue_name,
  reply_to,
  marshalling = {fun encoders:binary/1, fun decoders:binary/1}
}).

%%TODO: Avoid starting multiple connections
%% API
-export([start_link/1, is_alive/1, get_network_config/2, stop/1]).
-export([init_channel/1, send/3, reply/3, subscribe/2, handle_message/2, deliver_message/2, get_network_config/1, terminate/2]).

-define(DEFAULT_EXCHANGE, <<"antidote_exchange">>).

-spec start_link(Config :: channel_config()) ->
  {ok, Pid :: atom()} |
  ignore |
  {error, Reason :: atom()}.

start_link(Config) ->
  antidote_channel:start_link(Config#{module => channel_rabbitmq}).

-spec stop(Pid :: pid()) -> ok.
stop(Pid) ->
  antidote_channel:stop(Pid).

-spec get_network_config(Pattern :: atom(), ConfigMap :: map()) -> #rabbitmq_network{}.
get_network_config(pub_sub, ConfigMap) ->
  get_network_config(ConfigMap);

get_network_config(rpc, ConfigMap) ->
  get_network_config(ConfigMap);

get_network_config(_Other, _ConfigMap) ->
  {error, pattern_not_supported}.

-spec get_network_config(ConfigMap :: map()) -> #rabbitmq_network{}.
get_network_config(ConfigMap) ->
  Default = #rabbitmq_network{},
  Default#rabbitmq_network{
    username = maps:get(username, ConfigMap, Default#rabbitmq_network.username),
    password = maps:get(password, ConfigMap, Default#rabbitmq_network.password),
    virtual_host = maps:get(virtual_host, ConfigMap, Default#rabbitmq_network.virtual_host),
    host = maps:get(host, ConfigMap, Default#rabbitmq_network.host),
    port = maps:get(port, ConfigMap, Default#rabbitmq_network.port),
    channel_max = maps:get(channel_max, ConfigMap, Default#rabbitmq_network.channel_max),
    frame_max = maps:get(frame_max, ConfigMap, Default#rabbitmq_network.frame_max),
    heartbeat = maps:get(heartbeat, ConfigMap, Default#rabbitmq_network.heartbeat),
    connection_timeout = maps:get(connection_timeout, ConfigMap, Default#rabbitmq_network.connection_timeout),
    ssl_options = maps:get(ssl_options, ConfigMap, Default#rabbitmq_network.ssl_options),
    % List of functions
    auth_mechanisms = maps:get(auth_mechanisms, ConfigMap, Default#rabbitmq_network.auth_mechanisms),
    % List
    client_properties = maps:get(client_properties, ConfigMap, Default#rabbitmq_network.client_properties),
    % List
    socket_options = maps:get(socket_options, ConfigMap, Default#rabbitmq_network.socket_options),
    marshalling = maps:get(marshalling, ConfigMap, Default#rabbitmq_network.marshalling),
    rpc_queue_name = maps:get(rpc_queue_name, ConfigMap, Default#rabbitmq_network.rpc_queue_name),
    remote_rpc_queue_name = maps:get(remote_rpc_queue_name, ConfigMap, Default#rabbitmq_network.remote_rpc_queue_name)
  }.


%%%===================================================================
%%% Callbacks
%%%===================================================================
%TODO: SEPARATE OPENING A CONNECTION FROM OPENING A CHANNEL

% Routing with no topic is not supported.
% It might conflict with existing namespaces.
%init_channel(#pub_sub_channel_config{topics = [], namespace = <<>>}) ->
%  {error, not_supported};

init_channel(#pub_sub_channel_config{
  handler = Handler,
  topics = Topics,
  namespace = Namespace0,
  network_params = #rabbitmq_network{
    marshalling = Marshalling
  } = NetworkParams
} = Config) ->
  Namespace =
    case Namespace0 of
      <<>> -> ?DEFAULT_EXCHANGE;
      _ -> Namespace0
    end,

  Res = get_channel(NetworkParams),

  case Res of
    {ok, Connection, Channel} ->
      amqp_channel:register_return_handler(Channel, self()),
      QueueParams = #'queue.declare'{exclusive = true},
      Tags = case Topics of
               [] -> []; %fanout_declare(Endpoint, Namespace, QueueParams);
               _ ->
                 {ok, Queues} = direct_routing_declare(Channel, Namespace, Topics, QueueParams),
                 lists:foldl(fun(Q, TAcc) ->
                   {ok, Tag} = subscribe_queue(Channel, Q, self(), false),
                   [Tag | TAcc] end, [], Queues)
             end,
      State = #channel_state{
        config = Config,
        channel = Channel,
        connection = Connection,
        handler = Handler,
        exchange = Namespace,
        subscriber_tags = Tags,
        %%pub_sub is always async
        async = true,
        auto_ack = false,
        marshalling = Marshalling
      },
      case trigger_event(chan_started, #{channel => self()}, State) of
        ok -> {ok, State};
        {chan_started, _} -> {ok, State};
        {chan_closed, _} -> {error, channel_closed} % TODO: Is this correct?
      end;
    Other -> {error, Other}
  end;

%TODO: test exclusive and load balanced queues. Test add additional subscribed topics (see subscribe method)
%RPC Server
init_channel(#rpc_channel_config{
  handler = Handler,
  load_balanced = LoadBalanced,
  network_params = #rabbitmq_network{
    marshalling = Marshalling,
    rpc_queue_name = Name,
    prefetchCount = Prefetch
  } = NetworkParams
} = Config) when Name =/= undefined ->
  AutoAck = false,
  Res = get_channel(NetworkParams),
  case Res of
    {ok, Connection, Channel} ->
      amqp_channel:register_return_handler(Channel, self()),
      QueueName = create_queue(Channel, Name, not LoadBalanced, Prefetch),
      case QueueName of
        Name ->
          {ok, Tag} = subscribe_queue(Channel, Name, self(), AutoAck),
          State = #channel_state{
            config = Config,
            channel = Channel,
            connection = Connection,
            handler = Handler,
            exchange = <<"">>,
            subscriber_tags = [Tag],
            %%server is always async
            async = true,
            auto_ack = AutoAck,
            marshalling = Marshalling
          },
      case trigger_event(chan_started, #{channel => self()}, State) of
        ok -> {ok, State};
        {chan_started, _} -> {ok, State};
        {chan_closed, _} -> {error, channel_closed} % TODO: Is this correct?
      end;
      {error, _} = Error -> Error
      end;
    Other -> {error, Other}
  end;

%RPC Client
init_channel(#rpc_channel_config{
  handler = Handler,
  async = Async,
  network_params = #rabbitmq_network{
    marshalling = Marshalling,
    prefetchCount = Prefetch,
    remote_rpc_queue_name = RRPC_Name
  } = NetworkParams
} = Config) ->
  AutoAck = true,
  Res = get_channel(NetworkParams),
  case Res of
    {ok, Connection, Channel} ->
      amqp_channel:register_return_handler(Channel, self()),
      QueueName = create_queue(Channel, <<"">>, true, Prefetch),
      case QueueName of
        {error, _} = Error -> Error;
        Name ->
          {ok, Tag} = subscribe_queue(Channel, Name, self(), AutoAck),
          State = #channel_state{
            config = Config,
            channel = Channel,
            connection = Connection,
            handler = Handler,
            exchange = <<"">>,
            subscriber_tags = [Tag],
            async = Async,
            auto_ack = AutoAck,
            marshalling = Marshalling,
            rpc_queue_name = RRPC_Name,
            reply_to = QueueName
          },
      case trigger_event(chan_started, #{channel => self()}, State) of
        ok -> {ok, State};
        {chan_started, _} -> {ok, State};
        {chan_closed, _} -> {error, channel_closed} % TODO: Is this correct?
      end
    end;
    Other -> {error, Other}
  end;

init_channel(_Config) ->
  {error, bad_configuration}.

subscribe(Topics, #channel_state{channel = Channel, subscriber_tags = Tags} = State) ->
  TagsF = lists:foldl(
    fun(Topic, TagsAcc) ->
      {ok, Tag} = subscribe_queue(Channel, Topic, self(), false),
      [Tag | TagsAcc]
    end, Tags, Topics),
  {ok, State#channel_state{subscriber_tags = TagsF}}.

%TODO unsubscribe

send(#pub_sub_msg{topic = Topic} = Msg, _Params, #channel_state{channel = Channel, exchange = Exchange, marshalling = {Func, _}} = State) ->
  Publish = #'basic.publish'{exchange = Exchange, routing_key = Topic},
  amqp_channel:cast(Channel, Publish, #amqp_msg{payload = antidote_channel_utils:marshal(Msg, Func)}),
  {ok, State};

send(#rpc_msg{} = Msg, _Params, #channel_state{channel = Channel, exchange = Exchange, rpc_queue_name = QueueName, reply_to = ReplyName, marshalling = {Func, _}} = State) ->
  Publish = #'basic.publish'{exchange = Exchange, routing_key = QueueName},
  amqp_channel:cast(Channel, Publish, #amqp_msg{payload = antidote_channel_utils:marshal(Msg, Func), props = #'P_basic'{reply_to = ReplyName, correlation_id = random_correlation_id()}}),
  {ok, State};

send(Msg, _Params, State) -> {{error, {message_not_supported, Msg}}, State}.

reply(RId, Reply, #channel_state{channel =
Channel, pending = Pending, marshalling = {Func, _}, exchange = Exchange} = State) ->
  Res = case maps:find(RId, Pending) of
          {ok, #{props := #'P_basic'{correlation_id = CId, reply_to = RoutingKey}}} ->
            Msg = #rpc_msg{request_id = RId, reply_payload = Reply},
            Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
            amqp_channel:cast(Channel, Publish, #amqp_msg{payload = antidote_channel_utils:marshal(Msg, Func), props = #'P_basic'{correlation_id = CId}});
          R -> R
        end,
  {Res, State#channel_state{pending = maps:remove(RId, Pending)}}.

handle_message(
    #internal_msg{payload = #pub_sub_msg{} = Payload, meta = #{meta := #'basic.deliver'{delivery_tag = Tag}}},
    #channel_state{channel = Channel, handler = Handler, auto_ack = AutoAck} = State) ->
  _ = emit_event(Handler, Payload),
  if not AutoAck ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag})
  end,
  {ok, State};

handle_message(
    #internal_msg{payload = #rpc_msg{request_id = RId, request_payload = R} = Payload, meta = #{meta := #'basic.deliver'{delivery_tag = Tag}} = Meta},
    #channel_state{channel = Channel, handler = Handler, pending = Pending, auto_ack = AutoAck} = State) when R =/= undefined ->
  _ = emit_event(Handler, Payload),
  if not AutoAck ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag})
  end,
  {ok, State#channel_state{pending = Pending#{RId => Meta}}};

%Always auto_ack
handle_message(
    #internal_msg{
      payload = #rpc_msg{request_id = _RId, reply_payload = R} = Payload
    },
    #channel_state{handler = Handler} = State) when R =/= undefined ->
  _ = emit_event(Handler, Payload),
  {ok, State}.

emit_event(Handler, Payload) ->
  Handler ! Payload.

terminate(Reason, #channel_state{channel = Channel, subscriber_tags = Ts} = State) ->
  lists:foreach(fun(T) ->
    _ = amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = T})
                end, Ts),
  {chan_closed, _ } = trigger_event(chan_closed, #{reason => Reason}, State),
  amqp_channel:close(Channel).
%amqp_connection:close(Connection).


deliver_message({#'basic.deliver'{} = Meta, #'amqp_msg'{payload = Msg, props = Props}} = M, #channel_state{marshalling = {_, Func}} = _State) ->
  {deliver, #internal_msg{payload = antidote_channel_utils:unmarshal(Msg, Func), meta = #{meta => Meta, props => Props}}, M};
deliver_message(#'basic.consume_ok'{} = M, _) -> {do_nothing, M};
deliver_message(_, _) -> {error, bad_request}.


-spec is_alive(NetworkParams :: term()) -> true | false.
is_alive(#rabbitmq_network{} = NetworkParams) ->
  Res = get_connection(NetworkParams),
  case Res of
    {ok, _} -> true;
    _ -> false
  end.

%%%===================================================================
%%% Private Functions
%%%===================================================================


% Using one queue per routing_key. Can use multiple routing keys per queue, instead.
% Need to check performance to compare.

%fanout_declare(Channel, ExchangeName, #'queue.declare'{} = Params) ->
%  declare_exchange(ExchangeName, Channel, <<"fanout">>),
%  #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Params),
%  Binding = #'queue.bind'{
%    queue = Queue,
%    exchange = ExchangeName
%  },
%  #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
% {ok, [Queue]}.

direct_routing_declare(Channel, ExchangeName, RoutingKeys, #'queue.declare'{} = Params) ->
  ok = declare_exchange(ExchangeName, Channel, <<"direct">>),
  Queues = lists:foldl(fun(RK, Qs) ->
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Params),
    Binding = #'queue.bind'{
      queue = Queue,
      exchange = ExchangeName,
      routing_key = RK},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    [Queue | Qs] end, [], RoutingKeys),
  {ok, Queues}.

create_queue(Channel, Name, Exclusive, PrefetchCount) ->
  Queue = #'queue.declare'{queue = Name, exclusive = Exclusive, auto_delete = true},
  case amqp_channel:call(Channel, Queue) of
    #'queue.declare_ok'{queue = Name1} ->
      amqp_channel:call(Channel, #'basic.qos'{prefetch_count = PrefetchCount}),
      Name1;
    #'amqp_error'{} = Error -> {error, Error}
  end.

declare_exchange(<<>>, Channel, Type) ->
  declare_exchange(?DEFAULT_EXCHANGE, Channel, Type);
declare_exchange(ExchangeName, Channel, Type) ->
  Exchange = #'exchange.declare'{exchange = ExchangeName, type = Type},
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exchange),
  ok.

%TODO: Maybe set prefetch count here also
subscribe_queue(Channel, Queue, Subscriber, AutoAck) ->
  Sub = #'basic.consume'{queue = Queue, no_ack = AutoAck},
  #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Sub, Subscriber),
  {ok, Tag}.

init_or_get_connection_mgr() ->
  case whereis(rabbitmq_connection_mgr) of
    undefined ->
      {ok, Pid} = rabbitmq_connection_mgr:start_link(), Pid;
    Pid -> Pid
  end.

get_connection(NetworkParams) ->
  Pid = init_or_get_connection_mgr(),
  gen_server:call(Pid, {get_connection, get_amqp_params(NetworkParams)}).

get_channel(NetworkParams) ->
  case get_connection(NetworkParams) of
    {ok, Con} ->
      CRes = amqp_connection:open_channel(Con),
      case CRes of
        {ok, Endpt} -> {ok, Con, Endpt};
        ErrorChan -> ErrorChan
      end;
    ErrorCon -> ErrorCon
  end.

get_amqp_params(#rabbitmq_network{
  password = Pa, virtual_host = V, host = H, port = Po, channel_max = C, frame_max = F, heartbeat = Hb,
  connection_timeout = CT, ssl_options = S, auth_mechanisms = A, client_properties = CP, socket_options = SO
}) ->
  #amqp_params_network{
    password = Pa, virtual_host = V, host = inet_parse:ntoa(H), port = Po, channel_max = C, frame_max = F, heartbeat = Hb,
    connection_timeout = CT, ssl_options = S, auth_mechanisms = A, client_properties = CP, socket_options = SO
  }.

trigger_event(Event, Attributes, #channel_state{async = true, handler = Handler}) ->
  case Handler of
    undefined -> ok; %TODO this seems to silently swallow messages?
    _ -> Handler ! {Event, Attributes}
  end;

trigger_event(_Event, _Attributes, #channel_state{}) -> ok.


random_correlation_id() ->
  base64:encode(integer_to_binary(erlang:unique_integer())).