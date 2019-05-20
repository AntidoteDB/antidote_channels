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
-record(channel_state, {namespace, topic, namespace_topic, subscriber, context, publishers, current}).

%% API
-export([start_link/1, publish/2, stop/1]).
-export([init_channel/1, publish_async/3, handle_subscription/3, event_for_message/1]).


-spec start_link(Config :: channel_config()) ->
  {ok, Pid :: atom()} |
  ignore |
  {error, Reason :: atom()}.

start_link(Config) ->
  antidote_channel:start_link(?MODULE, Config).

stop(Pid) ->
  antidote_channel:stop(Pid).

publish(Pid, Msg) ->
  antidote_channel:publish_async(Pid, term_to_binary(Msg)).

%%TODO: Create supervisor for channels?
init_channel(#pub_sub_channel_config{
  topic = Topic,
  namespace = Namespace,
  network_params = #zmq_params{
    port = Port,
    pubAddresses = Pubs
  },
  subscriber = Process}
) ->
  try
    {ok, Context} = erlzmq:context(),
    {ok, Publisher} = erlzmq:socket(Context, pub),
    ConnString = connection_string({"*", Port}),
    ok = erlzmq:bind(Publisher, ConnString),

    TopicString = binary_join([Namespace, Topic], <<".">>),

    Subs = lists:foldl(fun(Address, AddressList) ->
      {ok, Subscriber} = erlzmq:socket(Context, [sub, {active, true}]),
      ConnStringI = connection_string(Address),
      ok = erlzmq:connect(Subscriber, ConnStringI),
      ok = erlzmq:setsockopt(Subscriber, subscribe, TopicString),
      [Subscriber | AddressList] end, [], Pubs),

    %%Need to send a message to the socket so it starts working
    erlzmq:send(Publisher, <<"init">>),
    timer:sleep(500),

    {ok, Publisher, #channel_state{context = Context, subscriber = Process, namespace = Namespace, topic = Topic, publishers = Subs, namespace_topic = TopicString, current = waiting}}
  catch
    failed_to_create_zmq_socket:E -> {error, E}
  end;

init_channel(_Config) ->
  {error, bad_configuration1}.

publish_async(Msg, Channel, #channel_state{namespace_topic = NT} = State) ->
  ok = erlzmq:send(Channel, NT, [sndmore]),
  ok = erlzmq:send(Channel, Msg),
  {ok, State}.

handle_subscription(#message{payload = {zmq, _Socket, NT, [rcvmore]}}, _Channel, #channel_state{namespace_topic = NT, current = waiting} = State) ->
  {ok, State#channel_state{current = receiving}};
handle_subscription(#message{payload = {zmq, _Socket, Msg, _Flags}}, _Channel, #channel_state{subscriber = S, current = receiving} = State) ->
  gen_server:call(S, binary_to_term(Msg)),
  {ok, State}.

%terminate(Channel, #channel_state{subscriber = S, context = C, publishers = P} = State) ->

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
