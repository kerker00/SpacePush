-module(spacepush_apns_http2_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TOPIC, {<<"https://http2-test.example/">>, <<"space">>}).
token(N) -> binary:copy(integer_to_binary(N), 64).
now_ms() -> erlang:system_time(millisecond).

lifecycle_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun goaway_does_not_replay_postponed_deliveries/0,
        fun timeout_cancels_stream_and_keeps_successor/0,
        fun partial_response_waits_for_fin/0,
        fun disconnect_backs_off_and_keeps_pending_work/0
    ]}.

setup() ->
    spacepush_test_util:setup_env("apns-http2"),
    application:set_env(spacepush, notification_cooldown_ms, 0),
    application:set_env(spacepush, delivery_ttl_ms, 60000),
    [begin {ok, Pid} = M:start_link(), unlink(Pid) end || M <- [spacepush_registry, spacepush_outbox]],
    ok = spacepush_registry:register(token(1), sandbox, [?TOPIC]).

cleanup(_) -> [gen_server:stop(M) || M <- [spacepush_outbox, spacepush_registry]].

state(Connection) ->
    #{keys => #{sandbox => unused}, connections => #{sandbox => #{pid => Connection, up => true}},
        tokens => #{sandbox => {<<"synthetic-provider-token">>, erlang:system_time(second)}},
        paused => #{}, renew => #{}, topic => <<"http2.test">>, request_timeout_ms => 15000,
        in_flight => #{}, max_in_flight => 2}.

enqueue(To) -> spacepush_outbox:enqueue([{?TOPIC, <<"HTTP2 test">>, closed, To}]).
wake(State) -> {noreply, Updated} = spacepush_apns:handle_cast(wake, State), Updated.
info(Message, State) -> {noreply, Updated} = spacepush_apns:handle_info(Message, State), Updated.

headers(#{server := Server}) ->
    receive {http2_headers, Server, Stream, Headers} -> {Stream, Headers} after 2000 -> error(no_http2_headers) end.

expire(State, Ref) ->
    Flight = maps:get(in_flight, State),
    Request = (maps:get(Ref, Flight))#{deadline := now_ms() - 1},
    info(tick, State#{in_flight := Flight#{Ref := Request}}).

closing_error(Connection, Ref) ->
    receive {gun_error, Connection, Ref, closing} = Message -> Message after 2000 -> error(post_not_rejected_while_closing) end.

goaway_does_not_replay_postponed_deliveries() ->
    Fixture = spacepush_http2_fixture:start(),
    #{connection := Connection, monitor := Monitor, server := Server} = Fixture,
    try
        ok = enqueue(open),
        Active = wake(state(Connection)),
        [FirstRef] = maps:keys(maps:get(in_flight, Active)),
        {Stream, _} = headers(Fixture),
        spacepush_http2_fixture:frame(Fixture, cow_http2:goaway(Stream, no_error, <<>>)),
        spacepush_http2_fixture:wait_state(Connection, closing),
        %% The sender has not received gun_down, and still considers this connection up.
        ok = spacepush_registry:register(token(2), sandbox, [?TOPIC]),
        ok = enqueue(closed),
        Closing = wake(Active),
        [SecondRef] = maps:keys(maps:remove(FirstRef, maps:get(in_flight, Closing))),
        Rejected = info(closing_error(Connection, SecondRef), Closing),
        ?assertEqual([FirstRef], maps:keys(maps:get(in_flight, Rejected))),
        ok = spacepush_registry:unregister(token(2)),
        TimedOut = expire(Rejected, FirstRef),
        [SuccessorRef] = maps:keys(maps:get(in_flight, TimedOut)),
        NoReplay = info(closing_error(Connection, SuccessorRef), TimedOut),
        ?assertEqual(#{}, maps:get(in_flight, NoReplay)),
        spacepush_http2_fixture:close(Fixture),
        Down = receive {'DOWN', Monitor, process, Connection, _} = D -> D after 2000 -> error(connection_did_not_stop) end,
        Disconnected = info(Down, NoReplay),
        ?assertEqual(#{}, maps:get(connections, Disconnected)),
        receive {http2_headers, Server, _, _} -> error(stale_post_reached_closing_peer) after 0 -> ok end,
        %% A fresh connection selects only the current registration and latest state.
        Fresh = spacepush_http2_fixture:start(),
        #{connection := FreshConnection} = Fresh,
        try
            [ok = spacepush_outbox:retry(Key, Id, now_ms()) ||
                #{key := Key, id := Id} <- spacepush_outbox:due(now_ms() + 5000, [], 10)],
            Sent = wake(Disconnected#{connections := #{sandbox => #{pid => FreshConnection, up => true}}}),
            {FreshStream, FreshHeaders} = headers(Fresh),
            ?assertEqual(<<"/3/device/", (token(1))/binary>>, proplists:get_value(<<":path">>, FreshHeaders)),
            [#{delivery := #{payload := Payload}}] = maps:values(maps:get(in_flight, Sent)),
            ?assertMatch(#{<<"state">> := <<"closed">>}, json:decode(Payload)),
            spacepush_http2_fixture:frame(Fresh, cow_http2:headers(FreshStream, fin, <<16#88>>)),
            [Ref] = maps:keys(maps:get(in_flight, Sent)),
            Response = receive {gun_response, FreshConnection, Ref, fin, 200, _} = R -> R after 2000 -> error(no_response) end,
            info(Response, Sent),
            ?assertEqual(0, spacepush_outbox:pending())
        after spacepush_http2_fixture:stop(Fresh) end
    after spacepush_http2_fixture:stop(Fixture) end.

timeout_cancels_stream_and_keeps_successor() ->
    Fixture = spacepush_http2_fixture:start(),
    #{connection := Connection, server := Server} = Fixture,
    try
        ok = enqueue(open),
        Sent = wake(state(Connection)),
        [Ref] = maps:keys(maps:get(in_flight, Sent)),
        {Stream, _} = headers(Fixture),
        ok = enqueue(closed),
        Successor = expire(Sent, Ref),
        receive {http2_frame, Server, {rst_stream, Stream, cancel}} -> ok after 2000 -> error(stream_not_cancelled) end,
        {_Stream2, _} = headers(Fixture),
        ?assertEqual(1, map_size(maps:get(in_flight, Successor))),
        ?assertEqual(Successor, info({gun_response, Connection, Ref, fin, 200, []}, Successor)),
        [#{delivery := #{payload := Payload}}] = maps:values(maps:get(in_flight, Successor)),
        ?assertMatch(#{<<"state">> := <<"closed">>}, json:decode(Payload)),
        ?assertEqual(1, spacepush_outbox:pending())
    after spacepush_http2_fixture:stop(Fixture) end.

disconnect_backs_off_and_keeps_pending_work() ->
    Fixture = spacepush_http2_fixture:start(),
    #{connection := Connection, monitor := Monitor} = Fixture,
    try
        ok = enqueue(open),
        Sent = wake(state(Connection)),
        headers(Fixture),
        Before = now_ms(),
        spacepush_http2_fixture:close(Fixture),
        Down = receive {'DOWN', Monitor, process, Connection, _} = D -> D after 2000 -> error(no_disconnect) end,
        Updated = info(Down, Sent),
        ?assertEqual(#{}, maps:get(in_flight, Updated)),
        ?assertEqual(1, spacepush_outbox:pending()),
        ?assertEqual([], spacepush_outbox:due(Before + 4999, [], 10)),
        ?assertEqual(Updated, wake(Updated)),
        ?assertMatch(#{sandbox := #{attempts := 1}}, maps:get(connection_retry, Updated))
    after spacepush_http2_fixture:stop(Fixture) end.

partial_response_waits_for_fin() ->
    Fixture = spacepush_http2_fixture:start(),
    #{connection := Connection} = Fixture,
    try
        ok = enqueue(open),
        Sent = wake(state(Connection)),
        [Ref] = maps:keys(maps:get(in_flight, Sent)),
        {Stream, _} = headers(Fixture),
        spacepush_http2_fixture:frame(Fixture, cow_http2:headers(Stream, nofin, <<16#88>>)),
        Response = receive {gun_response, Connection, Ref, nofin, 200, _} = R -> R after 2000 -> error(no_partial_headers) end,
        WithHeaders = info(Response, Sent),
        ?assertEqual(1, spacepush_outbox:pending()),
        spacepush_http2_fixture:frame(Fixture, cow_http2:data(Stream, nofin, <<"{">>)),
        First = receive {gun_data, Connection, Ref, nofin, <<"{">>} = D -> D after 2000 -> error(no_partial_body) end,
        Partial = info(First, WithHeaders),
        ?assertEqual(1, map_size(maps:get(in_flight, Partial))),
        ?assertEqual(1, spacepush_outbox:pending()),
        spacepush_http2_fixture:frame(Fixture, cow_http2:data(Stream, fin, <<"}">>)),
        Last = receive {gun_data, Connection, Ref, fin, <<"}">>} = D2 -> D2 after 2000 -> error(no_final_body) end,
        Completed = info(Last, Partial),
        ?assertEqual(#{}, maps:get(in_flight, Completed)),
        ?assertEqual(0, spacepush_outbox:pending())
    after spacepush_http2_fixture:stop(Fixture) end.
