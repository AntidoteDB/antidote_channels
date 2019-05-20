-module(rabbitmq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([groups/0, all/0, init_per_testcase/2, end_per_testcase/2, init_per_group/2, end_per_group/2, send_receive_multi_test/1]).
-export([send_receive_test/1]).

groups() -> [{multiple_subscribers, [], [send_receive_multi_test]}].
all() -> [send_receive_test, {group, multiple_subscribers}].


-define(PUB_SUB, #pub_sub_channel_config{
  network_params = #amqp_params{}
}).

init_per_testcase(send_receive_test, Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_rabbitmq:start_link(CConfig),
  [{subscriber, Pid1}, {channel, Pid2} | Config];

%%TODO: Need to unlink processes in order to user init_per_group
init_per_testcase(send_receive_multi_test, Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig1 = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_rabbitmq:start_link(CConfig1),
  {ok, Pid3} = basic_consumer:start_link(),
  CConfig2 = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid3},
  {ok, Pid4} = channel_rabbitmq:start_link(CConfig2),
  [{subscriber_channel1, {Pid1, Pid2}}, {subscriber_channel2, {Pid3, Pid4}} | Config].

end_per_testcase(send_receive_test, Config) ->
  Pid = ?config(channel, Config),
  channel_rabbitmq:stop(Pid),
  ok;

end_per_testcase(send_receive_multi_test, _Config) -> ok.

init_per_group(multiple_subscribers, Config) -> Config.

end_per_group(multiple_subscribers, _Config) -> ok.

send_receive_test(Config) ->
  Channel = ?config(channel, Config),
  Subscriber = ?config(subscriber, Config),
  channel_rabbitmq:publish(Channel, <<"Test">>),
  timer:sleep(500),
  {_, Buff} = sys:get_state(Subscriber),
  true = lists:member(<<"Test">>, Buff).

send_receive_multi_test(Config) ->
  {Sub1, Channel} = ?config(subscriber_channel1, Config),
  {Sub2, _Channel} = ?config(subscriber_channel2, Config),
  channel_rabbitmq:publish(Channel, <<"Test">>),
  timer:sleep(500),
  {_, Buff1} = sys:get_state(Sub1),
  {_, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test">>, Buff1),
  true = lists:member(<<"Test">>, Buff2).

%%TODO: termination
%%TODO: subscribe list of topics