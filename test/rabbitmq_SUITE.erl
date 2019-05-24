-module(rabbitmq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([groups/0, all/0, init_per_testcase/2, end_per_testcase/2, init_per_group/2, end_per_group/2]).
-export([init_close_test/1, send_receive_test/1, send_receive_multi_test/1, send_receive_multi_diff_test/1, send_receive_multi_topics/1, send_receive_nonamespace_test/1, send_receive_notopic_test/1]).

%TODO: Test exchange name conflicts.

groups() -> [
  {multiple_subscribers, [], [
    send_receive_multi_test,
    send_receive_multi_diff_test,
    send_receive_multi_topics
  ]}].

all() -> [
  init_close_test,
  send_receive_test,
  send_receive_nonamespace_test,
  send_receive_notopic_test,
  {group, multiple_subscribers}
].


-define(PORT, 5672).
-define(PUB_SUB, #{
  module => channel_rabbitmq,
  pattern => pub_sub,
  namespace => <<"test_env">>,
  network_params => #{port => ?PORT}
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

init_per_testcase(send_receive_test, Config) ->
  {ok, Sub} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#{topics => [<<"test_topic">>], subscriber => Sub},
  Chan = init_channel(CConfig),
  [{subscriber, Sub}, {channel, Chan} | Config];

init_per_testcase(send_receive_nonamespace_test, Config) ->
  {ok, Sub} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#{namespace => <<"any">>, topics => [<<"test_topic">>], subscriber => Sub},
  Chan = init_channel(CConfig),
  [{subscriber, Sub}, {channel, Chan} | Config];

init_per_testcase(send_receive_notopic_test, Config) ->
  {ok, Sub1} = basic_consumer:start_link(),
  {ok, Sub2} = basic_consumer:start_link(),
  CConfig1 = ?PUB_SUB#{namespace => <<"antidote_fanout">>, topics => [], subscriber => Sub1},
  CConfig2 = ?PUB_SUB#{namespace => <<"antidote_fanout">>, topics => [], subscriber => Sub2},
  Chan1 = init_channel(CConfig1),
  Chan2 = init_channel(CConfig2),
  [{subscriber1, Sub1}, {subscriber2, Sub2}, {channel1, Chan1}, {channel2, Chan2} | Config];

init_per_testcase(send_receive_multi_test, Config) ->
  CConfig1 = ?PUB_SUB#{topics => [<<"test_topic">>]},
  CConfig2 = ?PUB_SUB#{topics => [<<"test_topic">>]},
  Chan1 = init_channel(CConfig1, subscriber1, Config),
  Chan2 = init_channel(CConfig2, subscriber2, Config),
  [{channel1, Chan1}, {channel2, Chan2} | Config];

init_per_testcase(send_receive_multi_diff_test, Config) ->
  CConfig1 = ?PUB_SUB#{topics => [<<"test_topic1">>]},
  CConfig2 = ?PUB_SUB#{topics => [<<"test_topic2">>]},
  Chan1 = init_channel(CConfig1, subscriber1, Config),
  Chan2 = init_channel(CConfig2, subscriber2, Config),
  [{channel1, Chan1}, {channel2, Chan2} | Config];

init_per_testcase(send_receive_multi_topics, Config) ->
  CConfig1 = ?PUB_SUB#{topics => [<<"other_topic">>]},
  CConfig2 = ?PUB_SUB#{topics => [<<"multi_topic1">>, <<"multi_topic2">>]},
  Chan1 = init_channel(CConfig1, subscriber1, Config),
  Chan2 = init_channel(CConfig2, subscriber1, Config),
  [{channel1, Chan1}, {channel2, Chan2} | Config].

init_channel(ChannelConfig, SubscriberName, TestConfig) ->
  Sub = ?config(SubscriberName, TestConfig),
  CConfig = ChannelConfig#{subscriber => Sub},
  init_channel(CConfig).

init_channel(ChannelConfig) ->
  {ok, Chan} = antidote_channel:start_link(ChannelConfig),
  Chan.




end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(send_receive_test, Config) ->
  terminate_channel([?config(channel, Config)]);

end_per_testcase(send_receive_nonamespace_test, Config) ->
  terminate_channel([?config(channel, Config)]);

end_per_testcase(send_receive_notopic_test, Config) ->
  terminate_channel([?config(channel1, Config), ?config(channel2, Config)]);

end_per_testcase(send_receive_multi_test, Config) ->
  terminate_channel([?config(channel1, Config), ?config(channel2, Config)]);

end_per_testcase(send_receive_multi_diff_test, Config) ->
  terminate_channel([?config(channel1, Config), ?config(channel2, Config)]);

end_per_testcase(send_receive_multi_topics, Config) ->
  terminate_channel([?config(channel1, Config), ?config(channel2, Config)]).

terminate_channel(ChannelList) -> [antidote_channel:stop(X) || X <- ChannelList].





init_close_test(_Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#{namespace => <<"test_env">>, topics => [<<"test_topic">>], subscriber => Pid1},
  {ok, Pid2} = antidote_channel:start_link(CConfig),
  ok = antidote_channel:stop(Pid2).

send_receive_test(Config) ->
  Channel = ?config(channel, Config),
  Subscriber = ?config(subscriber, Config),
  antidote_channel:publish(Channel, <<"test_topic">>, <<"Test">>),
  timer:sleep(500),
  {_, Buff} = sys:get_state(Subscriber),
  true = lists:member(<<"Test">>, Buff).

send_receive_notopic_test(Config) ->
  Channel = ?config(channel1, Config),
  Subscriber1 = ?config(subscriber1, Config),
  Subscriber2 = ?config(subscriber2, Config),
  antidote_channel:publish(Channel, <<>>, <<"Test">>),
  timer:sleep(2000),
  {_, Buff1} = sys:get_state(Subscriber1),
  {_, Buff2} = sys:get_state(Subscriber2),
  true = lists:member(<<"Test">>, Buff1),
  true = lists:member(<<"Test">>, Buff2).


send_receive_nonamespace_test(Config) ->
  Channel = ?config(channel, Config),
  Subscriber = ?config(subscriber, Config),
  antidote_channel:publish(Channel, <<"test_topic">>, <<"Test">>),
  timer:sleep(2000),
  {_, Buff} = sys:get_state(Subscriber),
  true = lists:member(<<"Test">>, Buff).

send_receive_multi_test(Config) ->
  Channel = ?config(channel1, Config),
  Sub1 = ?config(subscriber1, Config),
  Sub2 = ?config(subscriber2, Config),
  antidote_channel:publish(Channel, <<"test_topic">>, <<"Test0">>),
  timer:sleep(500),
  {_, Buff1} = sys:get_state(Sub1),
  {_, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test0">>, Buff1),
  true = lists:member(<<"Test0">>, Buff2).

send_receive_multi_diff_test(Config) ->
  Channel = ?config(channel1, Config),
  Sub1 = ?config(subscriber1, Config),
  Sub2 = ?config(subscriber2, Config),
  antidote_channel:publish(Channel, <<"test_topic1">>, <<"Test1">>),
  antidote_channel:publish(Channel, <<"test_topic2">>, <<"Test2">>),
  timer:sleep(500),
  {_, Buff1} = sys:get_state(Sub1),
  {_, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test1">>, Buff1),
  false = lists:member(<<"Test2">>, Buff1),
  true = lists:member(<<"Test2">>, Buff2),
  false = lists:member(<<"Test1">>, Buff2).

send_receive_multi_topics(Config) ->
  Channel = ?config(channel1, Config),
  Sub = ?config(subscriber1, Config),
  antidote_channel:publish(Channel, <<"multi_topic1">>, <<"multi_topic1">>),
  antidote_channel:publish(Channel, <<"multi_topic2">>, <<"multi_topic2">>),
  antidote_channel:publish(Channel, <<"multi_topic3">>, <<"multi_topic3">>),
  timer:sleep(500),
  {_, Buff} = sys:get_state(Sub),
  true = lists:member(<<"multi_topic1">>, Buff),
  true = lists:member(<<"multi_topic2">>, Buff),
  false = lists:member(<<"multi_topic3">>, Buff).





