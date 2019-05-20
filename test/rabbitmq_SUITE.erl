-module(rabbitmq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([groups/0, all/0, init_per_testcase/2, end_per_testcase/2, init_per_group/2, end_per_group/2]).
-export([init_close_test/1, send_receive_test/1, send_receive_multi_test/1]).

groups() -> [{multiple_subscribers, [], [send_receive_multi_test]}].
all() -> [
  init_close_test,
  send_receive_test,
  {group, multiple_subscribers}
].


-define(PORT, 5672).
-define(PUB_SUB, #pub_sub_channel_config{
  network_params = #amqp_params{port = ?PORT}
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
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_rabbitmq:start_link(CConfig),
  [{subscriber, Pid1}, {channel, Pid2} | Config];

%%TODO: Need to unlink processes in order to use init_per_group
init_per_testcase(send_receive_multi_test, Config) ->
  Sub1 = ?config(subscriber1, Config),
  Sub2 = ?config(subscriber2, Config),

  CConfig1 = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Sub1},
  CConfig2 = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Sub2},

  {ok, Chan1} = channel_rabbitmq:start_link(CConfig1),
  {ok, Chan2} = channel_rabbitmq:start_link(CConfig2),

  [{subscriber_channel1, {Sub1, Chan1}}, {subscriber_channel2, {Sub2, Chan2}} | Config].




end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(send_receive_test, Config) ->
  Pid = ?config(channel, Config),
  channel_rabbitmq:stop(Pid),
  ok;

end_per_testcase(send_receive_multi_test, _Config) -> ok.





init_close_test(_Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_rabbitmq:start_link(CConfig),
  ok = channel_rabbitmq:stop(Pid2).

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