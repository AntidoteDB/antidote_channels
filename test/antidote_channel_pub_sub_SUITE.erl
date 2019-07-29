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

-module(antidote_channel_pub_sub_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").
-include_lib("test_includes.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

all() -> [
  init_close_test,
  send_receive_test,
  send_receive_nonamespace_test,
  send_receive_multi_test,
  send_receive_multi_diff_test,
  send_receive_multi_topics_test
].

-ifdef(TEST).
-compile(export_all).
-endif.


-define(PUB_SUB, #{
  pattern => pub_sub,
  namespace => <<"test_env">>
}).


%TODO: Add subscription during execution test

init_per_testcase(init_close_test, Config) -> Config;

init_per_testcase(send_receive_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun send_receive_rabbit_config/0},
    {channel_zeromq, fun send_receive_zero_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config);

init_per_testcase(send_receive_nonamespace_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun send_receive_nonamespace_rabbit_config/0},
    {channel_zeromq, fun send_receive_nonamespace_zero_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config);

init_per_testcase(send_receive_multi_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun send_receive_multi_rabbit_config/0},
    {channel_zeromq, fun send_receive_multi_zero_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config);

init_per_testcase(send_receive_multi_diff_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun send_receive_multi_diff_rabbit_config/0},
    {channel_zeromq, fun send_receive_multi_diff_zero_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config);

init_per_testcase(send_receive_multi_topics_test, Config) ->
  ImplConfigs = [
    {channel_rabbitmq, fun send_receive_multi_topics_rabbit_config/0},
    {channel_zeromq, fun send_receive_multi_topics_zero_config/0}
  ],
  test_utils:init_priv(ImplConfigs, Config).

end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(send_receive_test, Config) -> test_utils:close_priv(Config);

end_per_testcase(send_receive_nonamespace_test, Config) -> test_utils:close_priv(Config);

end_per_testcase(send_receive_multi_test, Config) -> test_utils:close_priv(Config);

end_per_testcase(send_receive_multi_diff_test, Config) -> test_utils:close_priv(Config);

end_per_testcase(send_receive_multi_topics_test, Config) -> test_utils:close_priv(Config).

init_close_test(_Config) ->
  Configs = [
    fun init_close_rabbit_config/0,
    fun init_close_zero_config/0
  ],
  lists:foreach(
    fun(ConfigFun) ->
      {C, _P} = ConfigFun(),
      {ok, Pid} = antidote_channel:start_link(C),
      ok = antidote_channel:stop(Pid)
    %Now closed with event
    %ok = basic_consumer:stop(P)
    end, Configs).

init_close_rabbit_config() ->
  {ok, Pid} = basic_consumer:start_link(),
  Config = ?PUB_SUB#{
    module => channel_rabbitmq,
    topics => [<<"test_topic">>],
    handler => Pid,
    network_params =>?RABBITMQ_PARAMS
  },
  {Config, Pid}.

init_close_zero_config() ->
  {ok, Pid} = basic_consumer:start_link(),
  Config = ?PUB_SUB#{
    module => channel_zeromq,
    topics => [<<"test_topic">>],
    handler => Pid,
    network_params => ?ZEROMQ_SUBSCRIBER_PARAMS
  },
  {Config, Pid}.

send_receive_test(Config) -> test_utils:call_fun_per_module_config(?MODULE, send_receive_test, Config).

send_receive_test_fun({[Channel], [#{handler := Sub}]}) ->
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"test_topic">>, payload = <<"Test1">>}),
  timer:sleep(200),
  {_, _, Buff} = sys:get_state(Sub),
  true = lists:member(<<"Test1">>, Buff),
  false = lists:member(<<"init">>, Buff).

send_receive_rabbit_config() ->
  Client1Config = test_utils:get_subscriber_pub_sub_config(channel_rabbitmq, [<<"test_topic">>], fun basic_consumer:start_link/0, #{}),
  [Client1Config].

send_receive_zero_config() ->
  Client1Config = test_utils:get_subscriber_pub_sub_config(channel_zeromq, [<<"test_topic">>], fun basic_consumer:start_link/0, #{}),
  [Client1Config].

send_receive_nonamespace_test(Config) ->
  test_utils:call_fun_per_module_config(?MODULE, send_receive_nonamespace_test, Config).

send_receive_nonamespace_test_fun({[Channel], [#{handler := Sub}]}) ->
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"test_topic">>, payload = <<"Test">>}),
  timer:sleep(200),
  {_, _, Buff} = sys:get_state(Sub),
  true = lists:member(<<"Test">>, Buff).

send_receive_nonamespace_rabbit_config() ->
  {ok, Sub} = basic_consumer:start_link(),
  [?PUB_SUB#{
    module => channel_rabbitmq,
    namespace => <<>>,
    topics => [<<"test_topic">>],
    handler => Sub,
    network_params => ?RABBITMQ_PARAMS
  }].

send_receive_nonamespace_zero_config() ->
  {ok, Sub} = basic_consumer:start_link(),
  [?PUB_SUB#{
    module => channel_zeromq,
    namespace => <<>>,
    topics => [<<"test_topic">>],
    handler => Sub,
    network_params => ?ZEROMQ_SUBSCRIBER_PARAMS
  }].

send_receive_multi_test(Config) -> test_utils:call_fun_per_module_config(?MODULE, send_receive_multi_test, Config).

send_receive_multi_test_fun({[Channel | _], [#{handler := Sub1}, #{handler := Sub2}]}) ->
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"test_topic">>, payload = <<"Test0">>}),
  timer:sleep(200),
  {_, _, Buff1} = sys:get_state(Sub1),
  {_, _, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test0">>, Buff1),
  true = lists:member(<<"Test0">>, Buff2).

send_receive_multi_rabbit_config() ->
  Client1Config = test_utils:get_subscriber_pub_sub_config(channel_rabbitmq, [<<"test_topic">>], fun basic_consumer:start_link/0, #{}),
  Client2Config = test_utils:get_subscriber_pub_sub_config(channel_rabbitmq, [<<"test_topic">>], fun basic_consumer:start_link/0, #{}),
  [Client1Config, Client2Config].

send_receive_multi_zero_config() ->
  Client1Params = #{
    port => 7866,
    publishersAddresses => [{{127, 0, 0, 1}, 7866}]
  },

  Client2Params = #{
    port => 7867,
    publishersAddresses => [{{127, 0, 0, 1}, 7866}]
  },

  Client1Config = test_utils:get_subscriber_pub_sub_config(channel_zeromq, [<<"test_topic">>], fun basic_consumer:start_link/0, Client1Params),
  Client2Config = test_utils:get_subscriber_pub_sub_config(channel_zeromq, [<<"test_topic">>], fun basic_consumer:start_link/0, Client2Params),

  [Client1Config, Client2Config].

send_receive_multi_diff_test(Config) ->
  test_utils:call_fun_per_module_config(?MODULE, send_receive_multi_diff_test, Config).

send_receive_multi_diff_test_fun({[Channel | _], [#{handler := Sub1}, #{handler := Sub2}]}) ->
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"test_topic1">>, payload = <<"Test1">>}),
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"test_topic2">>, payload = <<"Test2">>}),
  timer:sleep(500),
  {_, _, Buff1} = sys:get_state(Sub1),
  {_, _, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test1">>, Buff1),
  false = lists:member(<<"Test2">>, Buff1),
  true = lists:member(<<"Test2">>, Buff2),
  false = lists:member(<<"Test1">>, Buff2).

send_receive_multi_diff_rabbit_config() ->
  Client1Config = test_utils:get_subscriber_pub_sub_config(channel_rabbitmq, [<<"test_topic1">>], fun basic_consumer:start_link/0, #{}),
  Client2Config = test_utils:get_subscriber_pub_sub_config(channel_rabbitmq, [<<"test_topic2">>], fun basic_consumer:start_link/0, #{}),
  [Client1Config, Client2Config].

send_receive_multi_diff_zero_config() ->
  Client1Params = #{
    port => 7866,
    publishersAddresses => [{{127, 0, 0, 1}, 7866}]
  },

  Client2Params = #{
    port => 7867,
    publishersAddresses => [{{127, 0, 0, 1}, 7866}]
  },

  Client1Config = test_utils:get_subscriber_pub_sub_config(channel_zeromq, [<<"test_topic1">>], fun basic_consumer:start_link/0, Client1Params),
  Client2Config = test_utils:get_subscriber_pub_sub_config(channel_zeromq, [<<"test_topic2">>], fun basic_consumer:start_link/0, Client2Params),

  [Client1Config, Client2Config].

send_receive_multi_topics_test(Config) ->
  test_utils:call_fun_per_module_config(?MODULE, send_receive_multi_topics, Config).

send_receive_multi_topics_fun({[Channel | _], [_, #{handler := Sub}]}) ->
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"multi_topic1">>, payload = <<"multi_topic1">>}),
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"multi_topic2">>, payload = <<"multi_topic2">>}),
  antidote_channel:send(Channel, #pub_sub_msg{topic = <<"multi_topic3">>, payload = <<"multi_topic3">>}),
  timer:sleep(500),
  {_, _, Buff} = sys:get_state(Sub),
  true = lists:member(<<"multi_topic1">>, Buff),
  true = lists:member(<<"multi_topic2">>, Buff),
  false = lists:member(<<"multi_topic3">>, Buff).

send_receive_multi_topics_rabbit_config() ->
  {ok, Sub1} = basic_consumer:start_link(),
  {ok, Sub2} = basic_consumer:start_link(),
  Config1 = ?PUB_SUB#{
    module => channel_rabbitmq,
    topics => [<<"some_topic">>],
    handler => Sub1,
    network_params => ?RABBITMQ_PARAMS
  },
  Config2 = ?PUB_SUB#{
    module => channel_rabbitmq,
    topics => [<<"multi_topic1">>, <<"multi_topic2">>],
    handler => Sub2,
    network_params => ?RABBITMQ_PARAMS
  },
  [Config1, Config2].

send_receive_multi_topics_zero_config() ->
  {ok, Sub1} = basic_consumer:start_link(),
  {ok, Sub2} = basic_consumer:start_link(),
  Config1 = ?PUB_SUB#{
    module => channel_zeromq,
    handler => Sub1,
    topics => [],
    network_params => #{port => 7866, publishersAddresses => [{{127, 0, 0, 1}, 7866}]}
  },
  Config2 = ?PUB_SUB#{
    module => channel_zeromq,
    topics => [<<"multi_topic1">>, <<"multi_topic2">>],
    handler => Sub2,
    network_params => #{port => 7867, publishersAddresses => [{{127, 0, 0, 1}, 7866}]}
  },
  [Config1, Config2].
