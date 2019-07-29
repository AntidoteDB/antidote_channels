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

-module(test_utils).
-author("vbalegas").
-include_lib("common_test/include/ct.hrl").
-include_lib("test_includes.hrl").


%% API
-export([get_client_rpc_config/4, get_server_rpc_config/4, get_subscriber_pub_sub_config/4, get_publisher_pub_sub_config/3, call_fun_per_module_config/3, init_priv/2, close_priv/1]).


%Retrieve list of {module, {channels, Config}}.
get_subscriber_pub_sub_config(Module, Topics, HandlerFun, NetworkParams) ->
  {ok, Handler} = HandlerFun(),
  ClientConfig = ?PUB_SUB_PUBLISHER_OR_SUBSCRIBER#{
    module => Module,
    topics => Topics,
    handler => Handler,
    network_params => maps:merge(?SUBSCRIBER_PARAMS(Module), NetworkParams)
  },
  ClientConfig.

get_publisher_pub_sub_config(Module, HandlerFun, NetworkParams) ->
  {ok, Handler} = HandlerFun(),
  ServerConfig = ?PUB_SUB_PUBLISHER_OR_SUBSCRIBER#{
    module => Module,
    handler => Handler,
    network_params => maps:merge(?SERVER_PARAMS(Module), NetworkParams)
  },
  ServerConfig.

get_client_rpc_config(Module, Async, HandlerFun, NetworkParams) ->
  {ok, Handler} = HandlerFun(),
  ClientConfig = ?RPC_CLIENT_OR_SERVER#{
    module => Module,
    async => Async,
    handler => Handler,
    network_params => maps:merge(?CLIENT_PARAMS(Module), NetworkParams)
  },
  ClientConfig.

get_server_rpc_config(Module, LoadBalanced, HandlerFun, NetworkParams) ->
  {ok, Handler} = HandlerFun(),
  ServerConfig = ?RPC_CLIENT_OR_SERVER#{
    module => Module,
    load_balanced => LoadBalanced,
    handler => Handler,
    network_params => maps:merge(?SERVER_PARAMS(Module), NetworkParams)
  },
  ServerConfig.

init_priv(ImplConfigs, Config) ->
  Modules = lists:map(fun({Mod, _}) -> Mod end, ImplConfigs),
  lists:foldl(
    fun({Module, ConfigFun}, ConfigAcc) ->
      ConfigList = ConfigFun(),
      Channels = lists:foldr(
        fun(Conf, Chs) ->
          {ok, Channel} = antidote_channel:start_link(Conf),
          [Channel | Chs]
        end, [], ConfigList),
      [{Module, {Channels, ConfigList}} | ConfigAcc]
    end, [{configs, Modules} | Config], ImplConfigs).

close_priv(Config) ->
  lists:foreach(
    fun(Module) ->
      {Channels, _Configs} = ?config(Module, Config),
      lists:foreach(
        fun(Ch) ->
          ok = antidote_channel:stop(Ch)
        end, Channels)

    %lists:foreach(
    %fun(#{handler := Sub}) ->
    %basic_consumer:stop(Sub)
    %end, Configs)

    end, ?config(configs, Config)),
  ok.

call_fun_per_module_config(MODULE, TestCase, Config) ->
  lists:foreach(
    fun(Module) ->
      ModuleConfig = ?config(Module, Config),
      FunctionName = io_lib:format("~s~s", [TestCase, '_fun']),
      erlang:apply(MODULE, list_to_atom(FunctionName), [ModuleConfig])
    end, ?config(configs, Config)).