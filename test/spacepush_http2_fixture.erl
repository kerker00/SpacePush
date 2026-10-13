-module(spacepush_http2_fixture).
-moduledoc "A local, controllable HTTP/2 peer for sender lifecycle tests.".
-export([start/0, stop/1, frame/2, close/1, wait_state/2]).

start() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {ip, {127, 0, 0, 1}}, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    Parent = self(),
    Server = spawn_link(fun() ->
        {ok, Socket} = gen_tcp:accept(Listen, 2000),
        {ok, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>} = gen_tcp:recv(Socket, 24, 2000),
        ok = gen_tcp:send(Socket, cow_http2:settings(#{})),
        ok = inet:setopts(Socket, [{active, once}]),
        loop(Socket, Parent, <<>>, cow_hpack:init())
    end),
    Opts = maps:remove(tls_opts, spacepush_apns:connection_options("127.0.0.1")),
    {ok, Connection} = gun:open("127.0.0.1", Port, Opts#{transport := tcp, http2_opts => #{closing_timeout => 2000}}),
    Monitor = monitor(process, Connection),
    receive {gun_up, Connection, http2} -> ok after 2000 -> error(http2_not_connected) end,
    #{server => Server, listen => Listen, connection => Connection, monitor => Monitor}.

stop(#{server := Server, listen := Listen, connection := Connection, monitor := Monitor}) ->
    gun:close(Connection),
    Server ! stop,
    gen_tcp:close(Listen),
    demonitor(Monitor, [flush]),
    ok.

frame(#{server := Server}, Frame) ->
    Ref = make_ref(),
    Server ! {frame, self(), Ref, Frame},
    receive {frame_sent, Ref} -> ok after 2000 -> error(frame_not_sent) end.

close(#{server := Server}) -> Server ! close.

wait_state(Connection, Wanted) ->
    wait_state(Connection, Wanted, erlang:monotonic_time(millisecond) + 2000).

wait_state(Connection, Wanted, Deadline) ->
    case sys:get_state(Connection) of
        {Wanted, _} -> ok;
        _ ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true -> timer:sleep(1), wait_state(Connection, Wanted, Deadline);
                false -> error(http2_state_timeout)
            end
    end.

loop(Socket, Parent, Buffer, Decoder) ->
    receive
        {tcp, Socket, Data} ->
            {Rest, Decoder1} = parse(<<Buffer/binary, Data/binary>>, Socket, Parent, Decoder),
            ok = inet:setopts(Socket, [{active, once}]),
            loop(Socket, Parent, Rest, Decoder1);
        {frame, From, Ref, Frame} ->
            ok = gen_tcp:send(Socket, Frame),
            From ! {frame_sent, Ref},
            loop(Socket, Parent, Buffer, Decoder);
        {tcp_closed, Socket} -> Parent ! {http2_closed, self()};
        close -> gen_tcp:close(Socket);
        stop -> gen_tcp:close(Socket)
    after 10000 -> gen_tcp:close(Socket)
    end.

parse(Buffer, Socket, Parent, Decoder) ->
    case cow_http2:parse(Buffer) of
        more -> {Buffer, Decoder};
        {ok, Frame, Rest} ->
            Decoder1 = case Frame of
                {settings, _} -> ok = gen_tcp:send(Socket, cow_http2:settings_ack()), Decoder;
                {ping, Data} -> ok = gen_tcp:send(Socket, cow_http2:ping_ack(Data)), Decoder;
                {headers, Stream, _Fin, head_fin, Block} ->
                    {Headers, Updated} = cow_hpack:decode(Block, Decoder),
                    Parent ! {http2_headers, self(), Stream, Headers},
                    Updated;
                _ -> Parent ! {http2_frame, self(), Frame}, Decoder
            end,
            parse(Rest, Socket, Parent, Decoder1)
    end.
