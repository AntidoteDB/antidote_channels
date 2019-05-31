-module(zeromq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([
  all/0,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  bind_exception_test/1,
  test_socket/1,
  basic_rpc_test/1
]).

all() -> [
  bind_exception_test,
  test_socket,
  basic_rpc_test
].

-define(PORT, 7866).
-define(PUB_SUB, #{
  module => channel_zeromq,
  pattern => pub_sub,
  network_params => #{host => {0, 0, 0, 0}, port => ?PORT, publishersAddresses => [{{127, 0, 0, 1}, ?PORT}]},
  namespace => <<"test_env">>
}).


init_per_testcase(bind_exception_test, Config) -> Config;

init_per_testcase(test_socket, Config) -> Config;

init_per_testcase(basic_rpc_test, Config) -> Config.

end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(bind_exception_test, _Config) -> ok;

end_per_testcase(test_socket, _Config) -> ok;

end_per_testcase(basic_rpc_test, _Config) -> ok.


bind_exception_test(_Config) ->
  process_flag(trap_exit, true),
  {ok, Ctx} = erlzmq:context(),
  {ok, Socket} = erlzmq:socket(Ctx, pub),
  erlzmq:bind(Socket, "tcp://*:" ++ integer_to_list(?PORT)),
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#{
    topics => [<<"test_topic">>],
    namespace => <<"test_env">>,
    handler => Pid1
  },
  {error, eaddrinuse} = antidote_channel:start_link(CConfig),
  erlzmq:close(Socket).

test_socket(_Config) ->
  %TODO improve test; Also check that it is working in Antidote
  false = antidote_channel:is_alive(channel_zeromq, {{127, 0, 0, 1}, ?PORT}).

basic_rpc_test(_Config) ->

  {ok, Client} = basic_consumer:start_link(),

  ClientConfig = #{
    module => channel_zeromq,
    pattern => rpc,
    async => true,
    handler => Client,
    network_params => #{
      remote_host => {127, 0, 0, 1},
      remote_port => ?PORT
    }
  },

  {ok, Server} = basic_server:start_link(),

  ServerConfig = #{
    module => channel_zeromq,
    pattern => rpc,
    load_balanced => true,
    handler => Server,
    network_params => #{
      host => {127, 0, 0, 1},
      port => ?PORT
    }
  },

  {ok, _ServerChan} = antidote_channel:start_link(ServerConfig),
  {ok, ClientChan} = antidote_channel:start_link(ClientConfig),

  _ReqId = antidote_channel:send(ClientChan, #rpc_msg{request_payload = foo}),
  timer:sleep(500),
  {_, _, Buff} = sys:get_state(Client),
  true = lists:member(bar, Buff).

% duplex_rpc_test()