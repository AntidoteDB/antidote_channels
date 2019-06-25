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

-module(antidote_channel_utils).
-include_lib("antidote_channel.hrl").

-export([marshal/2, unmarshal/2]).

-define(HEADER_LENGTH_BYTES, 4).

marshal(Msg, EncodeFun) ->
  {Header, Content} = get_header_and_content(Msg),
  HeaderBin = erlang:term_to_binary(Header),
  HeaderLen = byte_size(HeaderBin),
  LenBin = binary:encode_unsigned(HeaderLen),
  LenPad = <<0:(8 * (?HEADER_LENGTH_BYTES - byte_size(LenBin))), LenBin/binary>>,
  <<LenPad/binary, HeaderBin/binary, (EncodeFun(Content))/binary>>.

unmarshal(Frame, DecodeFun) ->
  <<LenAgain:?HEADER_LENGTH_BYTES/binary, Rest/binary>> = Frame,
  LenInt = binary:decode_unsigned(LenAgain),
  <<Header:LenInt/binary, Bin/binary>> = Rest,
  Res = get_header_and_content_back(erlang:binary_to_term(Header), DecodeFun(Bin)),
  Res.

get_header_and_content(#rpc_msg{request_payload = P, reply_payload = undefined} = R) ->
  {R#rpc_msg{request_payload = content}, P};
get_header_and_content(#rpc_msg{request_payload = undefined, reply_payload = P} = R) ->
  {R#rpc_msg{reply_payload = content}, P};
get_header_and_content(#pub_sub_msg{payload = P} = R) ->
  {R#pub_sub_msg{payload = content}, P}.

get_header_and_content_back(#rpc_msg{request_payload = content, reply_payload = undefined} = R, Payload) ->
  R#rpc_msg{request_payload = Payload};
get_header_and_content_back(#rpc_msg{request_payload = undefined, reply_payload = content} = R, Payload) ->
  R#rpc_msg{reply_payload = Payload};
get_header_and_content_back(#pub_sub_msg{payload = content} = R, Payload) ->
  R#pub_sub_msg{payload = Payload}.
