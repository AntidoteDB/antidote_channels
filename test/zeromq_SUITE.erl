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

-module(zeromq_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("antidote_channel.hrl").

-export([
  all/0,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  bind_exception_test/1
]).

all() -> [bind_exception_test].

-define(PORT, 7866).
-define(PUB_SUB, #{
  module => channel_zeromq,
  pattern => pub_sub,
  network_params => #{host => {0, 0, 0, 0}, port => ?PORT, publishersAddresses => [{{127, 0, 0, 1}, ?PORT}]},
  namespace => <<"test_env">>
}).

init_per_testcase(bind_exception_test, Config) -> Config.

end_per_testcase(bind_exception_test, _Config) -> ok.

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
