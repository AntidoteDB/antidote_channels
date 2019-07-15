%%%-------------------------------------------------------------------
%%% @author vbalegas
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Jul 2019 15:55
%%%-------------------------------------------------------------------
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