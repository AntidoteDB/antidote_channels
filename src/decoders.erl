%%%-------------------------------------------------------------------
%%% @author vbalegas
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jun 2019 10:31
%%%-------------------------------------------------------------------
-module(decoders).
-author("vbalegas").

%% API
-export([dummy/1, binary/1]).

dummy(Payload) -> Payload.

binary(Payload) -> binary_to_term(Payload).
