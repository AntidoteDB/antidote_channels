-module(rabbitmq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([
  all/0,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  test_socket/1,
  basic_rpc_test/1,
  rpc_wait_test/1,
  bin_rpc_test/1
]).

all() -> [
  test_socket,
  basic_rpc_test,
  rpc_wait_test,
  bin_rpc_test
].

-define(PORT, 5672).
-define(PUB_SUB, #{
  module => channel_zeromq,
  pattern => pub_sub,
  network_params => #{host => {0, 0, 0, 0}, port => ?PORT, publishersAddresses => [{{127, 0, 0, 1}, ?PORT}]},
  namespace => <<"test_env">>
}).

%TODO: Make unique test suite for rabbit and zero

init_per_testcase(bind_exception_test, Config) -> Config;

init_per_testcase(test_socket, Config) -> Config;

init_per_testcase(basic_rpc_test, Config) -> Config;

init_per_testcase(rpc_wait_test, Config) -> Config;

init_per_testcase(bin_rpc_test, Config) -> Config.

end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(bind_exception_test, _Config) -> ok;

end_per_testcase(test_socket, _Config) -> ok;

end_per_testcase(basic_rpc_test, _Config) -> ok;

end_per_testcase(rpc_wait_test, Config) -> Config;

end_per_testcase(bin_rpc_test, Config) -> Config.

test_socket(_Config) ->
  true = antidote_channel:is_alive(channel_rabbitmq, #rabbitmq_network{host = {127, 0, 0, 1}, port = ?PORT}).

basic_rpc_test(_Config) ->

  {ok, Client} = basic_consumer:start_link(),

  ClientConfig = #{
    module => channel_rabbitmq,
    pattern => rpc,
    async => true,
    handler => Client,
    network_params => #{
      remote_rpc_queue_name => <<"test_queue">>
    }
  },

  {ok, Server} = basic_server:start_link(),

  ServerConfig = #{
    module => channel_rabbitmq,
    pattern => rpc,
    load_balanced => true, % TODO: Make test with false, make test that uses load balancing round-robin (e.g. each server return a different number)
    handler => Server,
    network_params => #{
      rpc_queue_name => <<"test_queue">>
    }
  },

  {ok, ServerChan} = antidote_channel:start_link(ServerConfig),
  {ok, ClientChan} = antidote_channel:start_link(ClientConfig),

  _ReqId = antidote_channel:send(ClientChan, #rpc_msg{request_payload = foo}),
  timer:sleep(500),
  {_, _, Buff} = sys:get_state(Client),
  true = lists:member(bar, Buff),
  antidote_channel:stop(ServerChan),
  antidote_channel:stop(ClientChan).


rpc_wait_test(_Config) ->

  {ok, Client2} = basic_consumer:start_link(),

  Client1Config = #{
    module => channel_rabbitmq,
    pattern => rpc,
    async => false,
    network_params => #{
      remote_host => {127, 0, 0, 1},
      remote_port => ?PORT,
      remote_rpc_queue_name => <<"rpc_wait_test">>
    }
  },

  Client2Config = #{
    module => channel_rabbitmq,
    pattern => rpc,
    async => true,
    handler => Client2,
    network_params => #{
      remote_host => {127, 0, 0, 1},
      remote_port =>?PORT,
      remote_rpc_queue_name => <<"rpc_wait_test">>
    }
  },

  {ok, Server} = basic_server:start_link(),

  ServerConfig = #{
    module => channel_rabbitmq,
    pattern => rpc,
    load_balanced => true,
    handler => Server,
    network_params => #{
      host => {0, 0, 0, 0},
      port =>?PORT,
      rpc_queue_name => <<"rpc_wait_test">>
    }
  },

  {ok, ServerChan} = antidote_channel:start_link(ServerConfig),
  {ok, Client1Chan} = antidote_channel:start_link(Client1Config),
  {ok, Client2Chan} = antidote_channel:start_link(Client2Config),

  bar = antidote_channel:send(Client1Chan, #rpc_msg{request_payload = foo}, #{}),
  bar = antidote_channel:send(Client2Chan, #rpc_msg{request_payload = foo}, #{wait => true}),
  {_, _, Buff} = sys:get_state(Client2),
  false = lists:member(bar, Buff),
  antidote_channel:send(Client2Chan, #rpc_msg{request_payload = foo}),
  timer:sleep(500),
  {_, _, Buff1} = sys:get_state(Client2),
  true = lists:member(bar, Buff1),
  antidote_channel:stop(ServerChan),
  antidote_channel:stop(Client1Chan),
  antidote_channel:stop(Client2Chan).

bin_rpc_test(_Config) ->
  %TODO: Negotiate content type instead of using parameters.
  {ok, Client} = basic_consumer:start_link(),

  ClientConfig = #{
    module => channel_rabbitmq,
    pattern => rpc,
    async => false,
    handler => Client,
    network_params => #{
      remote_host => {127, 0, 0, 1},
      remote_port => ?PORT,
      marshalling => {fun encoders:dummy/1, fun decoders:dummy/1},
      remote_rpc_queue_name => <<"bin_rpc_test">>
    }
  },

  {ok, Server} = basic_server:start_link(),

  ServerConfig = #{
    module => channel_rabbitmq,
    pattern => rpc,
    load_balanced => true, % TODO: Make test with false, make test that uses load balancing round-robin (e.g. each server return a different number)
    handler => Server,
    network_params => #{
      host => {0, 0, 0, 0},
      port =>?PORT,
      marshalling => {fun encoders:dummy/1, fun decoders:dummy/1},
      rpc_queue_name => <<"bin_rpc_test">>
    }
  },

  {ok, ServerChan} = antidote_channel:start_link(ServerConfig),
  {ok, ClientChan} = antidote_channel:start_link(ClientConfig),

  Res = antidote_channel:send(ClientChan, #rpc_msg{request_payload = <<131, 100, 0, 3, 102, 111, 111>>}),
  bar = erlang:binary_to_term(Res),
  antidote_channel:stop(ServerChan),
  antidote_channel:stop(ClientChan).
