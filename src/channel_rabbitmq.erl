%%%-------------------------------------------------------------------
%%% @author vbalegas
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. May 2019 14:30
%%%-------------------------------------------------------------------
-module(channel_rabbitmq).
-include_lib("antidote_channel.hrl").

-behavior(antidote_channel).

%subscriber must be a gen_server. We can put proxy instead, to abstract the handler.
-record(channel_state, {exchange, subscriber, subscriber_tag}).

%% API
-export([start_link/1, publish/2, stop/1]).
-export([init_channel/1, publish_async/3, handle_subscription/3, event_for_message/1]).


-spec start_link(Config :: channel_config()) ->
  {ok, Pid :: atom()} |
  ignore |
  {error, Reason :: atom()}.

start_link(Config) ->
  antidote_channel:start_link(?MODULE, Config).

stop(Pid) ->
  antidote_channel:stop(Pid).

publish(Pid, Msg) ->
  antidote_channel:publish_async(Pid, Msg).

init_channel(#pub_sub_channel_config{
  topic = Topic,
  namespace = Namespace,
  network_params = #amqp_params{username = U, password = Pass, virtual_host = _V, host = H, port = Port},
  subscriber = Process}
) ->
  NetworkParams = #amqp_params_network{username = U, password = Pass, host = H, port = Port},
  Res = case amqp_connection:start(NetworkParams) of
          {ok, Connection} ->
            amqp_connection:open_channel(Connection);
          Error -> Error
        end,

  case Res of
    {ok, Channel} ->
      amqp_channel:register_return_handler(Channel, self()),
      QueueParams = #'queue.declare'{exclusive = true},
      {ok, Queue} = fanout_routing_declare(Channel, Namespace, Topic, QueueParams),
      {ok, Tag} = subscribe_queue(Channel, Queue, self()),
      {ok, Channel, #channel_state{subscriber = Process, exchange = Namespace, subscriber_tag = Tag}};
    Other -> {error, Other}
  end;

init_channel(_Config) ->
  {error, bad_configuration}.

publish_async(Msg, Channel, #channel_state{exchange = Exchange} = State) ->
  Publish = #'basic.publish'{exchange = Exchange},
  amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Msg}),
  {ok, State}.

handle_subscription(#message{payload = {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Content}}}, Channel, #channel_state{subscriber = S} = State) ->
  Resp = gen_server:call(S, Content),
  case Resp of
    {error, _Reason} -> amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag});
    _ -> amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag})
  end,
  {ok, State}.


event_for_message({#'basic.deliver'{}, #'amqp_msg'{}}) -> {ok, push_notification};
event_for_message({#'basic.consume_ok'{}, _}) -> {ok, do_nothing};
event_for_message(_) -> {error, bad_request}.


%%%===================================================================
%%% Private Functions
%%%===================================================================

fanout_routing_declare(Channel, ExchangeName, RoutingKey, #'queue.declare'{} = Params) ->
  Exchange = #'exchange.declare'{exchange = ExchangeName, type = <<"fanout">>},
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exchange),
  #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Params),
  Binding = #'queue.bind'{
    queue = Queue,
    exchange = ExchangeName,
    routing_key = RoutingKey},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
  {ok, Queue}.

subscribe_queue(Channel, Queue, Subscriber) ->
  Sub = #'basic.consume'{queue = Queue},
  #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Sub, Subscriber),
  {ok, Tag}.


