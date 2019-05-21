-module(zeromq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([groups/0, all/0, init_per_testcase/2, end_per_testcase/2, init_per_group/2, end_per_group/2]).
-export([init_close_test/1, bind_exception_test/1, send_receive_test/1, send_receive_multi_test/1, send_receive_multi_diff_test/1]).


groups() -> [
  {multiple_subscribers, [], [
    send_receive_multi_test,
    send_receive_multi_diff_test
  ]}].

all() -> [
  init_close_test,
  bind_exception_test,
  send_receive_test
  %{group, multiple_subscribers}
].

-define(PORT, 7866).
-define(PUB_SUB, #pub_sub_channel_config{
  network_params = #zmq_params{port = ?PORT, pubAddresses = [{{127, 0, 0, 1}, ?PORT}]},
  namespace = <<"test_env">>
}).


init_per_group(multiple_subscribers, Config) ->
  {ok, Subscriber1} = basic_consumer:start(),
  {ok, Subscriber2} = basic_consumer:start(),
  [{subscriber1, Subscriber1}, {subscriber2, Subscriber2} | Config].


end_per_group(multiple_subscribers, Config) ->
  Pid1 = ?config(subscriber1, Config),
  Pid2 = ?config(subscriber2, Config),
  basic_consumer:stop(Pid1),
  basic_consumer:stop(Pid2),
  ok.

init_per_testcase(init_close_test, Config) -> Config;

init_per_testcase(bind_exception_test, Config) -> Config;

init_per_testcase(send_receive_test, Config) ->
  {ok, Sub} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic">>], subscriber = Sub},
  Chan = initChannel(CConfig),
  [{subscriber, Sub}, {channel, Chan} | Config];

init_per_testcase(send_receive_multi_test, Config) ->
  CConfig1 = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic">>]},
  CConfig2 = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic">>]},
  Chan1 = initChannel(CConfig1, subscriber1, Config),
  Chan2 = initChannel(CConfig2, subscriber2, Config),
  [{channel1, Chan1}, {channel2, Chan2} | Config];

init_per_testcase(send_receive_multi_diff_test, Config) ->
  CConfig1 = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic1">>]},
  CConfig2 = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic2">>]},
  Chan1 = initChannel(CConfig1, subscriber1, Config),
  Chan2 = initChannel(CConfig2, subscriber2, Config),
  [{channel1, Chan1}, {channel2, Chan2} | Config].

initChannel(ChannelConfig, SubscriberName, TestConfig) ->
  Sub = ?config(SubscriberName, TestConfig),
  CConfig = ChannelConfig#pub_sub_channel_config{subscriber = Sub},
  initChannel(CConfig).

initChannel(ChannelConfig) ->
  {ok, Chan} = channel_zeromq:start_link(ChannelConfig),
  Chan.


end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(bind_exception_test, _Config) -> ok;

end_per_testcase(send_receive_test, Config) ->
  terminate_channel([?config(channel, Config)]);

end_per_testcase(send_receive_multi_test, Config) ->
  terminate_channel([?config(channel1, Config), ?config(channel2, Config)]);

end_per_testcase(send_receive_multi_diff_test, Config) ->
  terminate_channel([?config(channel1, Config), ?config(channel2, Config)]).

terminate_channel(ChannelList) -> [channel_zeromq:stop(X) || X <- ChannelList].



init_close_test(_Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic">>], namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_zeromq:start_link(CConfig),
  ok = channel_zeromq:stop(Pid2).

bind_exception_test(_Config) ->
  process_flag(trap_exit, true),
  {ok, Ctx} = erlzmq:context(),
  {ok, Socket} = erlzmq:socket(Ctx, pub),
  erlzmq:bind(Socket, "tcp://*:" ++ integer_to_list(?PORT)),
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic">>], namespace = <<"test_env">>, subscriber = Pid1},
  {error, eaddrinuse} = channel_zeromq:start_link(CConfig),
  erlzmq:close(Socket).

send_receive_test(Config) ->
  Channel = ?config(channel, Config),
  Subscriber = ?config(subscriber, Config),
  channel_zeromq:publish(Channel, <<"test_topic">>, <<"Test">>),
  timer:sleep(2000),
  {_, Buff} = sys:get_state(Subscriber),
  true = lists:member(<<"Test">>, Buff),
  false = lists:member(<<"init">>, Buff).

send_receive_multi_test(Config) ->
  Channel = ?config(channel1, Config),
  Sub1 = ?config(subscriber1, Config),
  Sub2 = ?config(subscriber2, Config),
  channel_zeromq:publish(Channel, <<"test_topic">>, <<"Test0">>),
  timer:sleep(500),
  {_, Buff1} = sys:get_state(Sub1),
  {_, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test0">>, Buff1),
  true = lists:member(<<"Test0">>, Buff2).

send_receive_multi_diff_test(Config) ->
  Channel = ?config(channel1, Config),
  Sub1 = ?config(subscriber1, Config),
  Sub2 = ?config(subscriber2, Config),
  channel_zeromq:publish(Channel, <<"test_topic1">>, <<"Test1">>),
  channel_zeromq:publish(Channel, <<"test_topic2">>, <<"Test2">>),
  timer:sleep(500),
  {_, Buff1} = sys:get_state(Sub1),
  {_, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test1">>, Buff1),
  false = lists:member(<<"Test2">>, Buff1),
  true = lists:member(<<"Test2">>, Buff2),
  false = lists:member(<<"Test1">>, Buff2).
