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
-record(channel_state, {namespace :: binary(), topics :: [binary()], handler :: pid(), context, pub, subs, current :: atom()}).

%% API
-export([start_link/1, publish/3, stop/1]).
-export([init_channel/1, publish_async/3, handle_subscription/2, event_for_message/1, terminate/2]).


-ifndef(TEST).
-define(LOG_INFO(X, Y), ct:print(X, Y)).
-define(LOG_INFO(X), logger:info(X)).
-endif.

-ifdef(TEST).
-define(LOG_INFO(X, Y), ct:print(X, Y)).
-define(LOG_INFO(X), ct:print(X)).
-endif.


-spec start_link(Config :: channel_config()) ->
  {ok, Pid :: atom()} |
  ignore |
  {error, Reason :: atom()}.

start_link(Config) ->
  antidote_channel:start_link(?MODULE, Config).


-spec stop(Pid :: pid()) -> ok.
stop(Pid) ->
  antidote_channel:stop(Pid).

-spec publish(Pid :: pid(), Topic :: binary(), Msg :: term()) -> ok.
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
      Subs = connect_to_publishers(Pubs, Namespace, Topics, Context),
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

publish_async(Topic, Msg, #channel_state{pub = Channel, namespace = Namespace} = State) ->
  TopicBinary = getTopicBinary(Namespace, Topic),
  ok = erlzmq:send(Channel, TopicBinary, [sndmore]),
  ok = erlzmq:send(Channel, Msg),
  {ok, State}.

handle_subscription(#message{payload = {zmq, _Socket, _, [rcvmore]}}, #channel_state{namespace = <<>>, topics = [], current = waiting} = State) ->
  {ok, State#channel_state{current = receiving}};

handle_subscription(#message{payload = {zmq, _Socket, Namespace, [rcvmore]}}, #channel_state{namespace = Namespace, topics = [], current = waiting} = State) ->
  {ok, State#channel_state{current = receiving}};

handle_subscription(#message{payload = {zmq, _Socket, NamespaceTopic, [rcvmore]}}, #channel_state{namespace = N, topics = T, current = waiting} = State) ->
  case getTopicTerm(NamespaceTopic) of
    {ok, {Namespace, Topic}} when Namespace == N ->
      case lists:member(Topic, T) of
        true -> {ok, State#channel_state{current = receiving}};
        false when T == [] -> {ok, State#channel_state{current = receiving}};
        false -> {ok, State}
      end;
    _ -> ?LOG_INFO("Received non-subscribed topic. Ignoring ~p.", [NamespaceTopic]), {ok, State}
  end;

handle_subscription(#message{payload = {zmq, _Socket, Msg, [rcvmore]}}, #channel_state{handler = S, current = receiving} = State) ->
  ok = gen_server:call(S, binary_to_term(Msg)),
  {ok, State};

handle_subscription(#message{payload = {zmq, _Socket, Msg, _Flags}}, #channel_state{handler = S, current = receiving} = State) ->
  ok = gen_server:call(S, binary_to_term(Msg)),
  {ok, State#channel_state{current = waiting}};

handle_subscription(Msg, State) ->
  ?LOG_INFO("Unhandled Message ~p", [Msg]),
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

connect_to_publishers(Pubs, Namespace, Topics, Context) ->
  lists:foldl(fun(Address, AddressList) ->
    {ok, Subscriber} = erlzmq:socket(Context, [sub, {active, true}]),
    ConnStringI = connection_string(Address),
    ok = erlzmq:connect(Subscriber, ConnStringI),
    case Topics of
      [] when Namespace == <<>> ->
        ok = erlzmq:setsockopt(Subscriber, subscribe, <<>>);
      [] ->
        ok = erlzmq:setsockopt(Subscriber, subscribe, Namespace);
      _ -> lists:foreach(
        fun(Topic) ->
          TopicBinary = getTopicBinary(Namespace, Topic),
          ok = erlzmq:setsockopt(Subscriber, subscribe, TopicBinary)
        end, Topics)
    end,

    [Subscriber | AddressList] end, [], Pubs).

connection_string({Ip, Port}) ->
  IpString = case Ip of
               "*" -> Ip;
               _ -> inet_parse:ntoa(Ip)
             end,
  lists:flatten(io_lib:format("tcp://~s:~p", [IpString, Port])).


getTopicBinary(<<>>, <<>>) ->
  <<>>;
getTopicBinary(undefined, Topic) ->
  <<<<".">>/binary, Topic/binary>>;
getTopicBinary(<<>>, Topic) ->
  <<<<".">>/binary, Topic/binary>>;
getTopicBinary(Namespace, undefined) ->
  <<Namespace/binary>>;
getTopicBinary(Namespace, <<>>) ->
  <<Namespace/binary>>;
getTopicBinary(Namespace, Topic) ->
  <<Namespace/binary, <<".">>/binary, Topic/binary>>.


getTopicTerm(NamespaceTopic) ->
  case string:split(NamespaceTopic, ".") of
    [Namespace, Topic] -> {ok, {<<Namespace/binary>>, <<Topic/binary>>}};
    [Namespace] -> {ok, {<<Namespace/binary>>, <<>>}};
    [[], Topic] -> {ok, {<<>>, <<Topic/binary>>}};
    _ -> {error, wrong_format}
  end.