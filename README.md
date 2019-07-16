# Antidote Channels

Antidote channels is a messaging middleware that provides different communication patterns on top of a common interface. Right now we support Publish/Subscribe and RPC channels over RabbitMQ and ZeroMQ.

# Configuration
Different channels types are initiated using configurations. Here is a publisher:
```erlang
Config = #{
	module => channel_rabbitmq,
	pattern => pub_sub,
	namespace => <<>>,
	network_params => #{
		host => {0,0,0,0},
		port => Port
	}
},
{ok, Channel} = antidote_channel:start_link(Config),
```
And here is the subscriber:
```erlang
Config = #{
	module => channel_rabbitmq,
	pattern => pub_sub,
	handler => self(),
	topics => [<<"TOPIC">>],
	namespace => <<>>,
	network_params => #{
		remote_host => {127,0,0,1},
		remote_port => Port
	}
},
{ok, Channel} = antidote_channel:start_link(Config);
```
To send a message:
```erlang
antidote_channel:send(Channel, #pub_sub_msg{topic = <<"TOPIC">>, payload = Message}),
```
Pub/Sub messages are delivered asynchronously (handler must be a gen_server).
```erlang
handle_cast(#pub_sub_msg{payload = Message}, State) -> ...
```

## Integrate your own messaging system

We provide an extensible interface through the antidote_channel behaviour.
```erlang
-callback init_channel(Config :: channel_config()) -> {ok, State :: channel_state()} | {error, Reason :: term()}.  
  
-callback send(Msg :: message(), Params :: map(), State :: channel_state()) -> {ok, State :: channel_state()}.  

-callback deliver(Info :: message_payload(), State :: channel_state()) -> {  
  event(), message_payload()} | {event(), internal_msg(), message_payload()} | {error, Reason :: atom()}.  

-callback reply(RequestId :: reference(), Reply :: any(), State :: channel_state()) -> any().
  
-callback handle_message(Msg :: internal_msg(), State :: channel_state()) -> {ok, NewState :: channel_state()} | {error, Reason :: atom()}.  
    
-callback is_alive(NetworkParams :: term()) -> true | false.  
```
