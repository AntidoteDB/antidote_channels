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

-module(rabbitmq_connection_mgr).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
  connections
}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) -> {ok, #state{connections = #{}}}.

handle_call({get_connection, Options}, _From, #state{connections = Connections} = State) ->
  HostId = get_host_id(Options),
  case maps:find(HostId, Connections) of
    {ok, Connection} -> {reply, {ok, Connection}, State};
    _ ->
      case open_connection(Options) of
        {ok, Connection} = Res ->
          {reply, Res, State#state{connections = #{HostId => Connection}}};
        Error ->
          {reply, Error, State}
      end
  end.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{connections = Connections}) ->
  maps:fold(fun(_, V, _) -> amqp_connection:close(V) end, undefined, Connections).

code_change(_OldVsn, Ctx, _Extra) ->
  {ok, Ctx}.

get_host_id(#amqp_params_network{host = Host, port = undefined}) ->
  Host;
get_host_id(#amqp_params_network{host = Host, port = Port}) ->
  Host ++ integer_to_list(Port).

open_connection(#amqp_params_network{} = Options) ->
  amqp_connection:start(Options).