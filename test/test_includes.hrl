-define(ZEROMQ_SERVER, {127, 0, 0, 1}).
-define(RABBITMQ_PORT, 5672).
-define(ZEROMQ_PORT, 7866).

-define(RPC_CLIENT_OR_SERVER, #{pattern => rpc}).
-define(PUB_SUB_PUBLISHER_OR_SUBSCRIBER, #{pattern => pub_sub}).

-define(RABBITMQ_PARAMS, #{port => ?RABBITMQ_PORT}).

-define(ZEROMQ_SERVER_PARAMS, #{port => ?ZEROMQ_PORT, host => ?ZEROMQ_SERVER}).
-define(ZEROMQ_CLIENT_PARAMS, #{remote_port => ?ZEROMQ_PORT, remote_host => ?ZEROMQ_SERVER}).
-define(ZEROMQ_SUBSCRIBER_PARAMS, #{port => ?ZEROMQ_PORT, publishersAddresses => [{{127, 0, 0, 1}, ?ZEROMQ_PORT}]}).



-define(SERVER_PARAMS(Mod),
  case Mod of
    channel_zeromq -> ?ZEROMQ_SERVER_PARAMS;
    channel_rabbitmq -> ?RABBITMQ_PARAMS
  end
).
-define(CLIENT_PARAMS(Mod),
  case Mod of
    channel_zeromq -> ?ZEROMQ_CLIENT_PARAMS;
    channel_rabbitmq -> ?RABBITMQ_PARAMS
  end
).

-define(SUBSCRIBER_PARAMS(Mod),
  case Mod of
    channel_zeromq -> ?ZEROMQ_SUBSCRIBER_PARAMS;
    channel_rabbitmq -> ?RABBITMQ_PARAMS
  end
).

-define(DUMMY_MARSHALLER, {fun encoders:dummy/1, fun decoders:dummy/1}).