-module(antidote_channel_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([init_close_test/1]).

all() -> [init_close_test].


-define(RABBITMQ_PORT, 5672).
-define(ZEROMQ_PORT, 7866).

-define(PUB_SUB, #{
  pattern => pub_sub,
  namespace => <<"test_env">>
}).

-define(RABBITMQ_PARAMS, #{port => ?RABBITMQ_PORT}).
-define(ZEROMQ_PARAMS, #{pubPort => ?ZEROMQ_PORT, publishersAddresses => [{{127, 0, 0, 1}, ?ZEROMQ_PORT}]}).

init_per_testcase(init_close_test, Config) -> Config.

end_per_testcase(init_close_test, _Config) -> ok.

init_close_test(_Config) ->
  {ok, Pid1} = basic_consumer:start_link(),
  RConfig = init_close_test_rabbit_config(Pid1),
  ZConfig = init_close_test_zero_config(Pid1),
  init_close(RConfig),
  init_close(ZConfig).

init_close(Config) ->
  {ok, Pid} = antidote_channel:start_link(Config),
  ok = antidote_channel:stop(Pid).

init_close_test_rabbit_config(Pid) ->
  ?PUB_SUB#{
    module => channel_rabbitmq,
    topics => [<<"test_topic">>],
    subscriber => Pid,
    network_params => ?RABBITMQ_PARAMS
  }.

init_close_test_zero_config(Pid) ->
  ?PUB_SUB#{
    module => channel_rabbitmq,
    topics => [<<"test_topic">>],
    subscriber => Pid,
    network_params => ?ZEROMQ_PARAMS
  }.