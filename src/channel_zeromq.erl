%%%-------------------------------------------------------------------
%%% @author vbalegas
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. May 2019 14:30
%%%-------------------------------------------------------------------
-module(channel_zeromq).
-include_lib("antidote_channel.hrl").

-behavior(antidote_channel).

%subscriber must be a gen_server. We can put proxy instead, to abstract the handler.
-record(channel_state, {namespace, topics, handler, context, pub, subs, current}).

%% API
-export([start_link/1, publish/3, stop/1]).
-export([init_channel/1, publish_async/3, handle_subscription/2, event_for_message/1, terminate/2]).


-spec start_link(Config :: channel_config()) ->
  {ok, Pid :: atom()} |
  ignore |
  {error, Reason :: atom()}.

start_link(Config) ->
  antidote_channel:start_link(?MODULE, Config).

stop(Pid) ->
  antidote_channel:stop(Pid).

publish(Pid, Topic, Msg) ->
  antidote_channel:publish_async(Pid, Topic, term_to_binary(Msg)).

%%TODO: Create supervisor for channels?
init_channel(#pub_sub_channel_config{
  topics = Topics,
  namespace = Namespace,
  network_params = #zmq_params{
    port = Port,
    pubAddresses = Pubs
  },
  subscriber = Process}
) ->
  {ok, Context} = erlzmq:context(),
  {ok, Publisher} = erlzmq:socket(Context, pub),
  ConnString = connection_string({"*", Port}),
  Res = erlzmq:bind(Publisher, ConnString),
  case Res of
    ok ->
      Subs = lists:foldl(fun(Address, AddressList) ->
        {ok, Subscriber} = erlzmq:socket(Context, [sub, {active, true}]),
        ConnStringI = connection_string(Address),
        ok = erlzmq:connect(Subscriber, ConnStringI),
        lists:foreach(
          fun(Topic) ->
            TopicString = binary_join([Namespace, Topic], <<".">>),
            ok = erlzmq:setsockopt(Subscriber, subscribe, TopicString)
          end, Topics),
        [Subscriber | AddressList] end, [], Pubs),

%%Need to send a message to the socket so it starts working
      erlzmq:send(Publisher, <<"init">>),
      timer:sleep(500),
      {ok, #channel_state{
        pub = Publisher,
        context = Context,
        handler = Process,
        namespace = Namespace,
        topics = Topics,
        subs = Subs,
        current = waiting
      }
      };
    Error -> Error
  end;

init_channel(_Config) ->
  {error, bad_configuration}.

publish_async(Topic, Msg, #channel_state{pub = Channel, namespace = N} = State) ->
  TopicString = binary_join([N, Topic], <<".">>),
  ok = erlzmq:send(Channel, TopicString, [sndmore]),
  ok = erlzmq:send(Channel, Msg),
  {ok, State}.

handle_subscription(#message{payload = {zmq, _Socket, NamespaceTopic, [rcvmore]}}, #channel_state{namespace = N, topics = T, current = waiting} = State) ->
  case string:split(NamespaceTopic, ".") of
    [Namespace, Topic] when Namespace == N ->
      case lists:member(Topic, T) of
        true -> {ok, State#channel_state{current = receiving}};
        false -> {ok, State}
      end;
    _ -> {ok, State}
  end;

handle_subscription(#message{payload = {zmq, _Socket, Msg, [rcvmore]}}, #channel_state{handler = S, current = receiving} = State) ->
  ok = gen_server:call(S, binary_to_term(Msg)),
  {ok, State};

handle_subscription(#message{payload = {zmq, _Socket, Msg, _Flags}}, #channel_state{handler = S, current = receiving} = State) ->
  ok = gen_server:call(S, binary_to_term(Msg)),
  {ok, State#channel_state{current = waiting}};

handle_subscription(Msg, State) ->
  logger:info("Unhandled Message ~p", [Msg]),
  {ok, State}.


%%What to do with Subscriber? --- do nothing.
terminate(_Reason, #channel_state{context = C, pub = Channel, subs = Subscriptions}) ->
  lists:foreach(fun(Pi) -> erlzmq:close(Pi) end, Subscriptions),
  erlzmq:close(Channel),
  erlzmq:term(C).


event_for_message({zmq, _Socket, _BinaryMsg, _Flags}) -> {ok, push_notification};
event_for_message(_) -> {error, bad_request}.


%%%===================================================================
%%% Private Functions
%%%===================================================================

-spec binary_join([binary()], binary()) -> binary().
binary_join([], _Sep) ->
  <<>>;
binary_join([Part], _Sep) ->
  Part;
binary_join(List, Sep) ->
  lists:foldr(fun(A, B) ->
    if
      bit_size(B) > 0 -> <<A/binary, Sep/binary, B/binary>>;
      true -> A
    end
              end, <<>>, List).

connection_string({Ip, Port}) ->
  IpString = case Ip of
               "*" -> Ip;
               _ -> inet_parse:ntoa(Ip)
             end,
  lists:flatten(io_lib:format("tcp://~s:~p", [IpString, Port])).
