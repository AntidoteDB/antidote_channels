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
-include_lib("common_test/include/ct.hrl").

-behavior(antidote_channel).

%subscriber must be a gen_server. We can put proxy instead, to abstract the handler.
-record(channel_state, {endpoint, connection, exchange, handler, subscriber_tags}).

%% API
-export([start_link/1, is_alive/2, get_network_config/2, stop/1]).
-export([init_channel/1, send/2, subscribe/2, handle_message/2, unmarshal/2, get_network_config/1, terminate/2]).

-ifndef(TEST).
-define(LOG_INFO(X, Y), logger:info(X, Y)).
-define(LOG_INFO(X), logger:info(X)).
-endif.

-ifdef(TEST).
-define(LOG_INFO(X, Y), lager:info(X, Y)).
-define(LOG_INFO(X), lager:info(X)).
-endif.


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

%is_alive(rabbitmq_channel, Address) ->
%  is_alive(Address).


-spec get_network_config(Pattern :: atom(), ConfigMap :: map()) -> #amqp_params_network{}.
get_network_config(pub_sub, ConfigMap) ->
  get_network_config(ConfigMap);

get_network_config(_Other, _ConfigMap) ->
  {error, pattern_not_supported}.

-spec get_network_config(ConfigMap :: map()) -> #amqp_params_network{}.
get_network_config(ConfigMap) ->
  Default = #amqp_params_network{},
  Default#amqp_params_network{
    username = maps:get(username, ConfigMap, Default#amqp_params_network.username),
    password = maps:get(password, ConfigMap, Default#amqp_params_network.password),
    virtual_host = maps:get(virtual_host, ConfigMap, Default#amqp_params_network.virtual_host),
    host = maps:get(host, ConfigMap, Default#amqp_params_network.host),
    port = maps:get(port, ConfigMap, Default#amqp_params_network.port),
    channel_max = maps:get(channel_max, ConfigMap, Default#amqp_params_network.channel_max),
    frame_max = maps:get(frame_max, ConfigMap, Default#amqp_params_network.frame_max),
    heartbeat = maps:get(heartbeat, ConfigMap, Default#amqp_params_network.heartbeat),
    connection_timeout = maps:get(connection_timeout, ConfigMap, Default#amqp_params_network.connection_timeout),
    ssl_options = maps:get(ssl_options, ConfigMap, Default#amqp_params_network.ssl_options),
    % List of functions
    auth_mechanisms = maps:get(auth_mechanisms, ConfigMap, Default#amqp_params_network.auth_mechanisms),
    % List
    client_properties = maps:get(client_properties, ConfigMap, Default#amqp_params_network.client_properties),
    % List
    socket_options = maps:get(socket_options, ConfigMap, Default#amqp_params_network.socket_options)
  }.


%%%===================================================================
%%% Callbacks
%%%===================================================================

% Routing with no topic is not supported.
% It might conflict with existing namespaces.
init_channel(#pub_sub_channel_config{topics = [], namespace = <<>>}) ->
  {error, not_supported};

init_channel(#pub_sub_channel_config{
  topics = Topics,
  namespace = Namespace0,
  network_params = #amqp_params_network{} = NetworkParams,
  handler = Process}
) ->
  Namespace =
    case Namespace0 of
      <<>> -> ?DEFAULT_EXCHANGE;
      _ -> Namespace0
    end,

  Res = case amqp_connection:start(NetworkParams) of
          {ok, Con} ->
            CRes = amqp_connection:open_channel(Con),
            case CRes of
              {ok, Endpt} -> {ok, Con, Endpt};
              ErrorChan -> ErrorChan
            end;
          ErrorCon -> ErrorCon
        end,

  case Res of
    {ok, Connection, Endpoint} ->
      amqp_channel:register_return_handler(Endpoint, self()),
      QueueParams = #'queue.declare'{exclusive = true},
      {ok, Queues} =
        case Topics of
          [] -> fanout_declare(Endpoint, Namespace, QueueParams);
          _ -> direct_routing_declare(Endpoint, Namespace, Topics, QueueParams)
        end,
      Tags = lists:foldl(fun(Q, TAcc) ->
        {ok, Tag} = subscribe_queue(Endpoint, Q, self()),
        [Tag | TAcc] end, [], Queues),
      trigger_event(chan_started, #{channel => self()}, Process),
      {ok, #channel_state{
        endpoint = Endpoint,
        connection = Connection,
        handler = Process,
        exchange = Namespace,
        subscriber_tags = Tags
      }};
    Other -> {error, Other}
  end;

init_channel(_Config) ->
  {error, bad_configuration}.

subscribe(_Topics, #channel_state{} = _State) -> {error, not_implemented}.

send(#pub_sub_msg{topic = Topic} = Msg, #channel_state{endpoint = Endpoint, exchange = Exchange} = State) ->
  Publish = #'basic.publish'{exchange = Exchange, routing_key = Topic},
  amqp_channel:cast(Endpoint, Publish, #amqp_msg{payload = marshal(Msg)}),
  {ok, State};

send(_Msg, State) -> {ok, State}.

handle_message(
    #internal_msg{payload = #pub_sub_msg{payload = Payload}, meta = #{delivery_tag := Tag}},
    #channel_state{endpoint = Endpoint, handler = Handler} = State) ->
  cast_handler(Handler, Payload),
  amqp_channel:cast(Endpoint, #'basic.ack'{delivery_tag = Tag}),
  {ok, State}.

%call_handler(Handler, Payload) ->
%  gen_server:call(Handler, Payload).

cast_handler(Handler, Payload) ->
  gen_server:cast(Handler, Payload).


terminate(Reason, #channel_state{connection = Connection, endpoint = Endpoint, subscriber_tags = Ts, handler = Handler}) ->
  lists:foreach(fun(T) ->
    amqp_channel:call(Endpoint, #'basic.cancel'{consumer_tag = T})
                end, Ts),

  trigger_event(chan_closed, #{reason => Reason}, Handler),

  amqp_channel:close(Endpoint),
  amqp_connection:close(Connection).


unmarshal({#'basic.deliver'{delivery_tag = Tag}, #'amqp_msg'{payload = Msg}} = M, _) ->
  {deliver, #internal_msg{payload = binary_to_term(Msg), meta = #{delivery_tag => Tag}}, M};
unmarshal(#'basic.consume_ok'{} = M, _) -> {do_nothing, M};
unmarshal(_, _) -> {error, bad_request}.

marshal(Msg) ->
  term_to_binary(Msg).


-spec is_alive(Pattern :: atom(), Address :: {inet:ip_address(), inet:port_number()}) -> true | false.
is_alive(_Pattern, _Address) ->
  %TODO
  false.

%%%===================================================================
%%% Private Functions
%%%===================================================================


% Using one queue per routing_key. Can use multiple routing keys per queue, instead.
% Need to check performance to compare.

fanout_declare(Channel, ExchangeName, #'queue.declare'{} = Params) ->
  declare_exchange(ExchangeName, Channel, <<"fanout">>),
  #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Params),
  Binding = #'queue.bind'{
    queue = Queue,
    exchange = ExchangeName
  },
  #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),

  {ok, [Queue]}.

direct_routing_declare(Channel, ExchangeName, RoutingKeys, #'queue.declare'{} = Params) ->
  declare_exchange(ExchangeName, Channel, <<"direct">>),
  Queues = lists:foldl(fun(RK, Qs) ->
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Params),
    Binding = #'queue.bind'{
      queue = Queue,
      exchange = ExchangeName,
      routing_key = RK},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    [Queue | Qs] end, [], RoutingKeys),
  {ok, Queues}.


declare_exchange(<<>>, Channel, Type) ->
  declare_exchange(?DEFAULT_EXCHANGE, Channel, Type);
declare_exchange(ExchangeName, Channel, Type) ->
  Exchange = #'exchange.declare'{exchange = ExchangeName, type = Type},
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exchange).

subscribe_queue(Channel, Queue, Subscriber) ->
  Sub = #'basic.consume'{queue = Queue},
  #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Sub, Subscriber),
  {ok, Tag}.

trigger_event(Event, Attributes, Handler) ->
  case Handler of
    undefined -> ok;
    _ -> gen_server:cast(Handler, {Event, Attributes})
  end.
