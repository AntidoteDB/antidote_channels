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

-include_lib("amqp_client/include/amqp_client.hrl").

-define(DEFAULT_ZMQ_PORT, 8086).

-record(message, {payload}).

-record(pub_sub_channel_config, {
  topics = [] :: [binary()],
  namespace = <<"default">> :: binary(),
  network_params :: term(),
  subscriber :: pid()
}).

-record(pub_channel_config, {
  namespace = <<"default">> :: binary(),
  network_params :: term()
  %% TODO: rpc = false :: boolean() % Use true to force process to wait for a response
}).

-record(zmq_params, {
  pubHost = "*",
  pubPort,
  publishersAddresses = []
}).

-record(pub_zmq_params, {
  host = "*",
  port = ?DEFAULT_ZMQ_PORT
}).

-record(sub_zmq_params, {
  publishers = []
}).


-record(amqp_params, {
  username = <<"guest">>,
  password = <<"guest">>,
  virtual_host = <<"/">>,
  host = "localhost",
  port = undefined
}).

-type channel() :: term().
-type channel_config() :: term().
%% TODO: restrict possible types
-type channel_state() :: term().

-type message() :: #message{payload :: message_payload()}.
-type message_params() :: term().

-type message_payload() :: {#'basic.deliver'{}, #'amqp_msg'{}}.