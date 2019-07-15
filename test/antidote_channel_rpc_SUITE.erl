-module(antidote_channel_rpc_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").
-include_lib("test_includes.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

all() -> [
  test_socket_test,
  basic_rpc_test,
  bin_rpc_test,
  rpc_wait_test
].

-ifdef(TEST).
-compile(export_all).
-endif.


init_per_testcase(test_socket_test, Config) -> Config;

init_per_testcase(basic_rpc_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun basic_rpc_rabbit_test_config/0},
    {channel_zeromq, fun basic_rpc_zero_test_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config);

init_per_testcase(bin_rpc_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun bin_rpc_rabbit_test_config/0},
    {channel_zeromq, fun bin_rpc_zero_test_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config);

init_per_testcase(rpc_wait_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun rpc_wait_rabbit_test_config/0},
    {channel_zeromq, fun rpc_wait_zero_test_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config).

end_per_testcase(test_socket_test, _Config) -> ok;

end_per_testcase(basic_rpc_test, Config) ->
  test_utils:close_priv(Config);

end_per_testcase(bin_rpc_test, Config) ->
  test_utils:close_priv(Config);

end_per_testcase(rpc_wait_test, Config) ->
  test_utils:close_priv(Config).

test_socket_test(_Config) ->
  true = antidote_channel:is_alive(channel_zeromq, #pub_sub_zmq_params{host = {127, 0, 0, 1}, port = ?ZEROMQ_PORT}),
  true = antidote_channel:is_alive(channel_rabbitmq, #rabbitmq_network{host = {127, 0, 0, 1}, port = ?RABBITMQ_PORT}).

basic_rpc_rabbit_test_config() ->
  ClientParams = #{
    remote_rpc_queue_name => <<"bin_rpc_test">>
  },
  ServerParams = #{
    rpc_queue_name => <<"bin_rpc_test">>
  },

  ClientConfig = test_utils:get_client_rpc_config(channel_rabbitmq, true, fun basic_consumer:start_link/0, ClientParams),
  ServerConfig = test_utils:get_server_rpc_config(channel_rabbitmq, true, fun basic_server:start_link/0, ServerParams),
  [ClientConfig, ServerConfig].

basic_rpc_zero_test_config() ->
  ClientOrServerParams = #{},

  ClientConfig = test_utils:get_client_rpc_config(channel_zeromq, true, fun basic_consumer:start_link/0, ClientOrServerParams),
  ServerConfig = test_utils:get_server_rpc_config(channel_zeromq, true, fun basic_server:start_link/0, ClientOrServerParams),
  [ClientConfig, ServerConfig].

basic_rpc_test(Config) -> test_utils:call_fun_per_module_config(?MODULE, basic_rpc_test, Config).

basic_rpc_test_fun({[ClientChan, _ServerChan], [#{handler := Client}, _]}) ->
  _ReqId = antidote_channel:send(ClientChan, #rpc_msg{request_payload = foo}),
  timer:sleep(500),
  {_, _, Buff} = sys:get_state(Client),
  true = lists:member(bar, Buff).




bin_rpc_rabbit_test_config() ->
  ClientParams = #{
    marshalling => ?DUMMY_MARSHALLER,
    remote_rpc_queue_name => <<"bin_rpc_test">>
  },
  ServerParams = #{
    marshalling => ?DUMMY_MARSHALLER,
    rpc_queue_name => <<"bin_rpc_test">>
  },

  ClientConfig = test_utils:get_client_rpc_config(channel_rabbitmq, false, fun basic_consumer:start_link/0, ClientParams),
  ServerConfig = test_utils:get_server_rpc_config(channel_rabbitmq, true, fun basic_server:start_link/0, ServerParams),
  [ClientConfig, ServerConfig].

bin_rpc_zero_test_config() ->
  ClientOrServerParams = #{marshalling => ?DUMMY_MARSHALLER},
  ClientConfig = test_utils:get_client_rpc_config(channel_zeromq, false, fun basic_consumer:start_link/0, ClientOrServerParams),
  ServerConfig = test_utils:get_server_rpc_config(channel_zeromq, true, fun basic_server:start_link/0, ClientOrServerParams),
  [ClientConfig, ServerConfig].

bin_rpc_test(Config) -> test_utils:call_fun_per_module_config(?MODULE, bin_rpc_test, Config).

bin_rpc_test_fun({[ClientChan | _], _}) ->
  Res = antidote_channel:send(ClientChan, #rpc_msg{request_payload = <<131, 100, 0, 3, 102, 111, 111>>}),
  bar = erlang:binary_to_term(Res).





rpc_wait_rabbit_test_config() ->
  ClientParams = #{remote_rpc_queue_name => <<"bin_rpc_test">>},
  ServerParams = #{rpc_queue_name => <<"bin_rpc_test">>},
  Client1Config = test_utils:get_client_rpc_config(channel_rabbitmq, false, fun basic_consumer:start_link/0, ClientParams),
  Client2Config = test_utils:get_client_rpc_config(channel_rabbitmq, true, fun basic_consumer:start_link/0, ClientParams),
  ServerConfig = test_utils:get_server_rpc_config(channel_rabbitmq, true, fun basic_server:start_link/0, ServerParams),
  [Client1Config, Client2Config, ServerConfig].

rpc_wait_zero_test_config() ->
  Client1Config = test_utils:get_client_rpc_config(channel_zeromq, false, fun basic_consumer:start_link/0, #{}),
  Client2Config = test_utils:get_client_rpc_config(channel_zeromq, true, fun basic_consumer:start_link/0, #{}),
  ServerConfig = test_utils:get_server_rpc_config(channel_zeromq, true, fun basic_server:start_link/0, #{}),
  [Client1Config, Client2Config, ServerConfig].

rpc_wait_test(Config) -> test_utils:call_fun_per_module_config(?MODULE, rpc_wait_test, Config).

rpc_wait_test_fun({[Client1Chan, Client2Chan, _], [_, #{handler := Client2}, _]}) ->
  bar = antidote_channel:send(Client1Chan, #rpc_msg{request_payload = foo}, #{}),
  bar = antidote_channel:send(Client2Chan, #rpc_msg{request_payload = foo}, #{wait => true}),
  {_, _, Buff} = sys:get_state(Client2),
  false = lists:member(bar, Buff),
  antidote_channel:send(Client2Chan, #rpc_msg{request_payload = foo}),
  timer:sleep(1000),
  {_, _, Buff1} = sys:get_state(Client2),
  true = lists:member(bar, Buff1).


%TODO: test server load-balancing
