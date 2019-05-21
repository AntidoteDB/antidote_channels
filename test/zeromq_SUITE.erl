-module(zeromq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([init_close_test/1, bind_exception_test/1, send_receive_test/1, send_receive_multi_test/1]).

all() -> [
  init_close_test,
  bind_exception_test,
  send_receive_test,
  send_receive_multi_test
].

-define(PORT, 7866).
-define(PUB_SUB, #pub_sub_channel_config{
  network_params = #zmq_params{port = ?PORT, pubAddresses = [{{127, 0, 0, 1}, ?PORT}]}
}).


init_per_testcase(init_close_test, Config) -> Config;

init_per_testcase(bind_exception_test, Config) -> Config;

init_per_testcase(send_receive_test, Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topics = [<<"test_topic">>], namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_zeromq:start_link(CConfig),
  [{subscriber, Pid1}, {channel, Pid2} | Config];

init_per_testcase(send_receive_multi_test, Config) ->
  {ok, Sub1} = basic_consumer:start(),
  {ok, Sub2} = basic_consumer:start(),

  CConfig1 = ?PUB_SUB#pub_sub_channel_config{
    namespace = <<"test_env">>,
    topics = [<<"test_topic">>],
    subscriber = Sub1,
    network_params = #zmq_params{port = 7861, pubAddresses = [{{127, 0, 0, 1}, 7861}]}
  },
  CConfig2 = ?PUB_SUB#pub_sub_channel_config{
    namespace = <<"test_env">>,
    topics = [<<"test_topic">>],
    subscriber = Sub2,
    network_params = #zmq_params{port = 7862, pubAddresses = [{{127, 0, 0, 1}, 7861}]}
  },

  {ok, Chan1} = channel_zeromq:start_link(CConfig1),
  {ok, Chan2} = channel_zeromq:start_link(CConfig2),

  [{subscriber_channel1, {Sub1, Chan1}}, {subscriber_channel2, {Sub2, Chan2}} | Config].





end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(bind_exception_test, _Config) -> ok;

end_per_testcase(send_receive_test, Config) ->
  Pid = ?config(channel, Config),
  ok = channel_zeromq:stop(Pid),
  ok;

end_per_testcase(send_receive_multi_test, _Config) -> ok.





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
  {Sub1, Channel} = ?config(subscriber_channel1, Config),
  {Sub2, _Channel} = ?config(subscriber_channel2, Config),
  channel_zeromq:publish(Channel, <<"test_topic">>, <<"Test0">>),
  timer:sleep(500),
  {_, Buff1} = sys:get_state(Sub1),
  {_, Buff2} = sys:get_state(Sub2),
  true = lists:member(<<"Test0">>, Buff1),
  true = lists:member(<<"Test0">>, Buff2).

%TODO: test socket creation exception in init b-y trying to binding on a socket that is already bound