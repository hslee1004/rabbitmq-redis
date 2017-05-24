%% Copyright (c) 2007-2017 Pivotal Software, Inc.
%% You may use this code for any purpose.
%% https://www.rabbitmq.com/erlang-client-user-guide.html
%% https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/erlang/receive.erl

-module(rabbit_metronome_worker).
-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([fire/0]).

-include_lib("eredis/include/eredis.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {channel, exchange}).
-define(RKFormat,
        "~4.10.0B.~2.10.0B.~2.10.0B.~1.10.0B.~2.10.0B.~2.10.0B.~2.10.0B").

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%---------------------------
% Gen Server Implementation
% --------------------------

init([]) ->
    {ok, Redis} = eredis:start_link(),
    log("init started..."),
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{}),
    log(io_lib:format("amqp connection: ~p ~n", [Connection])),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    log(io_lib:format("amqp channel: ~p ~n", [Channel])),
    {ok, Exchange} = application:get_env(rabbitmq_metronome, exchange),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = Exchange,
                                                   type = <<"topic">>}),
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel,
                                     #'queue.declare'{queue = <<"redis_queue">>}),
    amqp_channel:call(Channel, #'queue.bind'{queue = Queue,
                                             exchange = Exchange,
                                             routing_key = <<"*.*.*.*.*.*.*">>}),
    Pid = spawn(fun() -> loop(Redis) end),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue, no_ack = true}, Pid),
    Pid2 = spawn(fun() -> loop2(Redis) end),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue, no_ack = true}, Pid2),
    fire(),
    log("called subscribe"),
    {ok, <<"OK">>} = eredis:q(Redis, ["SET", "plugin-init", "started.."]),
    {ok, #state{channel = Channel, exchange = Exchange}}.

handle_call(_Msg, _From, State) ->
    {reply, unknown_command, State}.

handle_cast(fire, State = #state{channel = Channel, exchange = Exchange}) ->
    Properties = #'P_basic'{content_type = <<"text/plain">>, delivery_mode = 1},
    {Date={Year,Month,Day},{Hour, Min,Sec}} = erlang:universaltime(),
    DayOfWeek = calendar:day_of_the_week(Date),
    RoutingKey = list_to_binary(
                   io_lib:format(?RKFormat, [Year, Month, Day,
                                             DayOfWeek, Hour, Min, Sec])),
    Message = RoutingKey,
    BasicPublish = #'basic.publish'{exchange = Exchange,
                                    routing_key = RoutingKey},
    Content = #amqp_msg{props = Properties, payload = Message},
    amqp_channel:call(Channel, BasicPublish, Content),
    timer:apply_after(100, ?MODULE, fire, []),
    {noreply, State};

handle_cast(_, State) ->
    {noreply,State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_, #state{channel = Channel}) ->
    amqp_channel:call(Channel, #'channel.close'{}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

log(Msg) ->
  file:write_file("/var/log/rabbitmq/hslee.log", io_lib:fwrite("log1: ~p.\n", [Msg]), [append]).

log2(Msg) ->
  file:write_file("/var/log/rabbitmq/hslee.log", io_lib:fwrite("log2: ~p.\n", [Msg]), [append]).

%---------------------------

fire() ->
    gen_server:cast({global, ?MODULE}, fire).

%% https://www.rabbitmq.com/erlang-client-user-guide.html
%% https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/erlang/receive.erl

loop(Redis) ->
  %%log("loop_started.."),
  receive
    %% This is the first message received
    #'basic.consume_ok'{} ->
      loop(Redis);

    %% This is received when the subscription is cancelled
    #'basic.cancel_ok'{} ->
      ok;

    {#'basic.deliver'{}, #amqp_msg{payload = Body}} ->
      log(Body),
      io:format(" [x] Received ~p~n", [Body]),
      {ok, <<"OK">>} = eredis:q(Redis, ["SET", "from_rabbit", [Body]]),
      loop(Redis)
  end.

loop2(Redis) ->
  %%log2("loop2_started.."),
  receive
    %% This is the first message received
    #'basic.consume_ok'{} ->
      loop2(Redis);

    %% This is received when the subscription is cancelled
    #'basic.cancel_ok'{} ->
      ok;

    {#'basic.deliver'{}, #amqp_msg{payload = Body}} ->
      log2(Body),
      io:format(" [x] Received ~p~n", [Body]),
      {ok, <<"OK">>} = eredis:q(Redis, ["SET", "from_rabbit", [Body]]),
      loop2(Redis)
  end.
