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
  basic_rpc_test/1
]).

all() -> [
  test_socket,
  basic_rpc_test
].

-define(PORT, 5672).
-define(PUB_SUB, #{
  module => channel_zeromq,
  pattern => pub_sub,
  network_params => #{host => {0, 0, 0, 0}, port => ?PORT, publishersAddresses => [{{127, 0, 0, 1}, ?PORT}]},
  namespace => <<"test_env">>
}).


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
