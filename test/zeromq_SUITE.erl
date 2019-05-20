-module(zeromq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([init_close_test/1, bind_exception_test/1, send_receive_test/1]).

all() -> [
%  init_close_test,
  bind_exception_test
%  send_receive_test
].

-define(PORT, 7866).
-define(PUB_SUB, #pub_sub_channel_config{
  network_params = #zmq_params{port = ?PORT, pubAddresses = [{{127, 0, 0, 1}, ?PORT}]}
}).


init_per_testcase(init_close_test, Config) -> Config;

init_per_testcase(bind_exception_test, Config) -> Config;

init_per_testcase(send_receive_test, Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_zeromq:start_link(CConfig),
  [{subscriber, Pid1}, {channel, Pid2} | Config].





end_per_testcase(init_close_test, _Config) -> ok;

end_per_testcase(bind_exception_test, _Config) -> ok;

end_per_testcase(send_receive_test, Config) ->
  Pid = ?config(channel, Config),
  channel_zeromq:stop(Pid),
  ok.





init_close_test(_Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid1},
  {ok, Pid2} = channel_zeromq:start_link(CConfig),
  ok = channel_zeromq:stop(Pid2).

bind_exception_test(_Config) ->
  process_flag(trap_exit, true),
  {ok, Ctx} = erlzmq:context(),
  {ok, Socket} = erlzmq:socket(Ctx, pub),
  erlzmq:bind(Socket, "tcp://*:" ++ integer_to_list(?PORT)),
  {ok, Pid1} = basic_consumer:start_link(),
  CConfig = ?PUB_SUB#pub_sub_channel_config{topic = <<"test_topic">>, namespace = <<"test_env">>, subscriber = Pid1},
  {error, eaddrinuse} = channel_zeromq:start_link(CConfig).

send_receive_test(Config) ->
  Channel = ?config(channel, Config),
  Subscriber = ?config(subscriber, Config),
  channel_zeromq:publish(Channel, <<"Test">>),
  timer:sleep(2000),
  {_, Buff} = sys:get_state(Subscriber),
  true = lists:member(<<"Test">>, Buff),
  false = lists:member(<<"init">>, Buff).

%TODO: test socket creation exception in init b-y trying to binding on a socket that is already bound