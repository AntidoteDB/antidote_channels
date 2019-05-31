%%%-------------------------------------------------------------------
%%% @author vbalegas
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. May 2019 14:30
%%%-------------------------------------------------------------------
-module(channel_zeromq).
-include_lib("antidote_channel.hrl").

-behavior(antidote_channel).

%subscriber must be a gen_server. We can put proxy instead, to abstract the handler.
-record(channel_state, {
  config :: channel_config(),
  namespace = <<>> :: binary(),
  topics = [] :: [binary()],
  handler :: pid(),
  context,
  subs,
  endpoint
}).

%% API
-export([start_link/1, is_alive/2, get_network_config/2, stop/1]).
-export([init_channel/1, subscribe/2, send/2, handle_message/2, unmarshal/2, terminate/2]).


-ifndef(TEST).
-define(LOG_INFO(X, Y), logger:info(X, Y)).
-define(LOG_INFO(X), logger:info(X)).
-endif.

-ifdef(TEST).
-define(LOG_INFO(X, Y), lager:info(X, Y)).
-define(LOG_INFO(X), lager:info(X)).
-endif.

-define(ZMQ_TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(Config :: channel_config()) ->
  {ok, Pid :: atom()} |
  ignore |
  {error, Reason :: atom()}.

start_link(Config) ->
  antidote_channel:start_link(Config#{module => channel_zeromq}).

%-spec is_alive(ChannelType :: channel_type(), Address :: {inet:ip_address(), inet:port_number()}) -> true | false.

%is_alive(zeromq_channel, Address) ->
%  is_alive(Address).

-spec stop(Pid :: pid()) -> ok.

stop(Pid) ->
  antidote_channel:stop(Pid).

-spec get_network_config(Pattern :: atom(), ConfigMap :: map()) -> #pub_sub_zmq_params{} | #rpc_channel_zmq_params{} | {error, Reason :: atom()}.
get_network_config(pub_sub, ConfigMap) ->
  get_network_config_private(pub_sub, ConfigMap);

get_network_config(rpc, ConfigMap) ->
  get_network_config_private(rpc, ConfigMap);

get_network_config(_Other, _ConfigMap) ->
  {error, pattern_not_supported}.

-spec get_network_config_private(atom(), ConfigMap :: map()) -> #pub_sub_zmq_params{} | #rpc_channel_zmq_params{}.
get_network_config_private(pub_sub, ConfigMap) ->
  Default = #pub_sub_zmq_params{},
  Default#pub_sub_zmq_params{
    host = maps:get(host, ConfigMap, Default#pub_sub_zmq_params.host),
    port = maps:get(port, ConfigMap, Default#pub_sub_zmq_params.port),
    publishersAddresses = maps:get(publishersAddresses, ConfigMap, Default#pub_sub_zmq_params.publishersAddresses)
  };

get_network_config_private(rpc, ConfigMap) ->
  Default = #rpc_channel_zmq_params{},
  Default#rpc_channel_zmq_params{
    host = maps:get(host, ConfigMap, Default#rpc_channel_zmq_params.host),
    port = maps:get(port, ConfigMap, Default#rpc_channel_zmq_params.port),
    remote_host = maps:get(remote_host, ConfigMap, Default#rpc_channel_zmq_params.remote_host),
    remote_port = maps:get(remote_port, ConfigMap, Default#rpc_channel_zmq_params.remote_port)
  }.


%%%===================================================================
%%% Callbacks
%%%===================================================================

get_context() ->
  case whereis(zmq_context) of
    undefined ->
      ct:print("context undefined"),
      zmq_context:start_link(),
      zmq_context:get();
    _ -> ct:print("context defined ~p", [zmq_context:get()]), zmq_context:get()
  end.

%%TODO: Create supervisor for channels?
init_channel(#pub_sub_channel_config{
  topics = Topics,
  namespace = Namespace,
  network_params = #pub_sub_zmq_params{
    host = Host,
    port = Port,
    publishersAddresses = Pubs
  },
  handler = Handler} = Config
) ->
  Context = get_context(),

  Res =
    case Port of
      undefined -> {ok, undefined};
      _ ->
        {ok, Publisher} = erlzmq:socket(Context, pub),
        ConnString = connection_string({Host, Port}),
        Bind = erlzmq:bind(Publisher, ConnString),
        {Bind, Publisher}
    end,


  case Res of
    {ok, Endpoint} ->
      Subs = connect_to_publishers(Pubs, Namespace, Topics, Context),
      case Endpoint of
        undefined -> ok;
        _ ->
          erlzmq:send(Endpoint, <<>>),
          timer:sleep(100)
      end,
      trigger_event(chan_started, #{channel => self()}, Handler),
      {ok, #channel_state{
        config = Config,
        endpoint = Endpoint,
        context = Context,
        handler = Handler,
        namespace = Namespace,
        topics = Topics,
        subs = Subs
      }};
    {{error, _} = E, _} -> E;
    Error -> Error
  end;


init_channel(#rpc_channel_config{
  handler = Handler,
  async = true,
  network_params = #rpc_channel_zmq_params{
    remote_host = RHost,
    remote_port = RPort
  }
} = Config) ->
  Context = get_context(),
  {ok, Socket} = erlzmq:socket(Context, [req, {active, true}]),
  ok = erlzmq:setsockopt(Socket, rcvtimeo, ?ZMQ_TIMEOUT),
  ConnString = connection_string({RHost, RPort}),
  Connect = erlzmq:connect(Socket, ConnString),
  case Connect of
    ok ->
      trigger_event(chan_started, #{channel => self()}, Handler),
      {ok, #channel_state{
        config = Config,
        context = Context,
        handler = Handler,
        endpoint = Socket
      }};
    {{error, _} = E, _} -> E;
    Error -> Error
  end;

init_channel(#rpc_channel_config{
  handler = Handler,
  load_balanced = LoadBalanced,
  network_params = #rpc_channel_zmq_params{
    host = Host,
    port = Port
  }
} = Config) ->

  SocketType = case LoadBalanced of
                 true -> xrep;
                 false -> rep
               end,
  SocketType = xrep,

  Context = get_context(),
  {ok, Socket} = erlzmq:socket(Context, [SocketType, {active, true}]),
  ConnString = connection_string({Host, Port}),
  Bind = erlzmq:bind(Socket, ConnString),
  case Bind of
    ok ->
      trigger_event(chan_started, #{channel => self()}, Handler),
      {ok, #channel_state{
        config = Config,
        context = Context,
        handler = Handler,
        endpoint = Socket
      }};
    {{error, _} = E, _} -> E;
    Error -> Error
  end;

init_channel(Config) ->
  {error, {bad_configuration, Config}}.

subscribe(Topics, #channel_state{subs = Subs, namespace = Namespace} = State) ->
  lists:foreach(fun(Sub) ->
    subscribe_topics(Sub, Namespace, Topics) end, Subs),
  {ok, State}.

send(#pub_sub_msg{topic = Topic} = Msg, #channel_state{endpoint = Endpoint, namespace = Namespace} = State) ->
  TopicBinary = get_topic_with_namespace(Namespace, Topic),
  ok = erlzmq:send(Endpoint, TopicBinary, [sndmore]),
  ok = erlzmq:send(Endpoint, marshal(Msg)),
  {ok, State};

send(#rpc_msg{} = Msg, #channel_state{endpoint = Endpoint} = State) ->
  ok = erlzmq:send(Endpoint, marshal(Msg)),
  {ok, State}.

is_subscribed(NamespaceTopicIn, Namespace, Topics) ->
  case get_topic_term(NamespaceTopicIn, Namespace) of
    {ok, Topic} ->
      case lists:member(Topic, Topics) of
        true -> ok;
        false when Topics == [] -> ok;
        false -> nok
      end;
    _ -> ?LOG_INFO("Error parsing topic ~p.", [NamespaceTopicIn]), nok
  end.


handle_message(
    #internal_msg{
      payload = #pub_sub_msg{payload = Payload},
      meta = #{buffered := [_, {zmq, _, NamespaceTopic, _}]}
    },
    #channel_state{handler = S, namespace = Namespace, topics = Topics} = State) ->

  case is_subscribed(NamespaceTopic, Namespace, Topics) of
    ok -> {cast_handler(S, Payload), State};
    nok -> {{error, topic_not_subscribed}, State}
  end;

handle_message(
    #internal_msg{
      payload = #rpc_msg{request_id = RId, request_payload = Payload},
      meta = #{socket := Socket, buffered := [_, _, {zmq, _, Id, _}]}
    },
    #channel_state{handler = S} = State) ->

  Resp = call_handler(S, Payload),
  erlzmq:send(Socket, Id, [sndmore]),
  erlzmq:send(Socket, <<>>, [sndmore]),
  erlzmq:send(Socket, marshal(#rpc_msg{request_id = RId, reply_payload = Resp})),
  case Resp of
    stop -> {ok, State};%TODO
    _ -> {ok, State}
  end;

handle_message(
    #internal_msg{payload = #rpc_msg{reply_payload = Payload}},
    #channel_state{handler = S} = State) ->
  {cast_handler(S, Payload), State};

handle_message(Msg, State) -> {{error, {message_not_supported, Msg}, State}}.

%TODO: can handle Response to close channel.

call_handler(Handler, Payload) ->
  gen_server:call(Handler, Payload).

cast_handler(Handler, Payload) ->
  gen_server:cast(Handler, Payload).


%TODO: close context
terminate(Reason, #channel_state{context = _C, subs = Subs, endpoint = Endpoint, handler = Handler}) ->
  ?LOG_INFO("CALL TERMINATE"),
  case Subs of
    undefined -> ok;
    _ -> lists:foreach(fun(Pi) -> erlzmq:close(Pi) end, Subs)
  end,

  trigger_event(chan_closed, #{reason => Reason}, Handler),

  case Endpoint of
    undefined -> ok;
    _ -> erlzmq:close(Endpoint)
  end,

  case Endpoint of
    undefined -> ok;
    _ -> erlzmq:close(Endpoint)
  end.

%erlzmq:term(C).


unmarshal({zmq, _Socket, <<>>, [rcvmore]} = M, _State) ->
  {buffer, M};
unmarshal({zmq, _Socket, _IdOrNamespaceTopic, [rcvmore]} = M, _State) ->
  {buffer, M};
unmarshal({zmq, Socket, Msg, Flags} = M, _State) ->
  {deliver, #internal_msg{payload = binary_to_term(Msg), meta = #{socket => Socket, flags => Flags}}, M};
unmarshal(_, _) -> {error, bad_request}.

marshal(Msg) ->
  term_to_binary(Msg).

-spec is_alive(Pattern :: atom(), Attributes :: #{address => {inet:ip_address(), inet:port_number()}}) -> true | false.
is_alive(pub_sub, #{address := Address}) ->
  Context = get_context(),
  {ok, Socket} = erlzmq:socket(Context, [sub, {active, false}]),
  ok = erlzmq:connect(Socket, connection_string(Address)),
  ok = erlzmq:setsockopt(Socket, rcvtimeo, ?CONNECTION_TIMEOUT),
  ok = erlzmq:setsockopt(Socket, subscribe, <<>>),
  Res = erlzmq:recv(Socket),
  erlzmq:close(Socket),
  case Res of
    {ok, _} -> true;
    _ -> false
  end;

is_alive(rpc, #{address := Address, pingMsg := PingMsg}) ->
  Context = get_context(),
  {ok, Socket} = erlzmq:socket(Context, [req, {active, false}]),
  ok = erlzmq:connect(Socket, connection_string(Address)),
  ok = erlzmq:send(Socket, PingMsg),
  ok = erlzmq:setsockopt(Socket, rcvtimeo, ?CONNECTION_TIMEOUT),
  Res = erlzmq:recv(Socket),
  erlzmq:close(Socket),
  case Res of
    {ok, _} -> true;
    _ -> false
  end.

%%%===================================================================
%%% Private Functions
%%%===================================================================

connect_to_publishers(Pubs, Namespace, Topics, Context) ->
  lists:foldl(fun(Address, AddressList) ->
    {ok, Subscriber} = erlzmq:socket(Context, [sub, {active, true}]),
    ConnStringI = connection_string(Address),
    ok = erlzmq:connect(Subscriber, ConnStringI),
    subscribe_topics(Subscriber, Namespace, Topics),
    [Subscriber | AddressList] end, [], Pubs).


subscribe_topics(Subscriber, Namespace, Topics) ->
  case Topics of
    [] when Namespace == <<>> ->
      ok = erlzmq:setsockopt(Subscriber, subscribe, <<>>);
    [] ->
      ok = erlzmq:setsockopt(Subscriber, subscribe, Namespace);
    _ -> lists:foreach(
      fun(Topic) ->
        TopicBinary = get_topic_with_namespace(Namespace, Topic),
        ok = erlzmq:setsockopt(Subscriber, subscribe, TopicBinary)
      end, Topics)
  end.

connection_string({Ip, Port}) ->
  IpString = case Ip of
               "*" -> Ip;
               _ -> inet_parse:ntoa(Ip)
             end,
  lists:flatten(io_lib:format("tcp://~s:~p", [IpString, Port])).


get_topic_with_namespace(<<>>, Topic) ->
  Topic;
get_topic_with_namespace(Namespace, Topic) ->
  <<Namespace/binary, Topic/binary>>.


get_topic_term(NamespaceTopic, Namespace) ->
  case string:prefix(NamespaceTopic, Namespace) of
    nomatch -> {error, wrong_format};
    Topic -> {ok, Topic}
  end.

trigger_event(Event, Attributes, Handler) ->
  case Handler of
    undefined -> ok;
    _ -> gen_server:cast(Handler, {Event, Attributes})
  end.