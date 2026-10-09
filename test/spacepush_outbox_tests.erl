-module(spacepush_outbox_tests).
-include_lib("eunit/include/eunit.hrl").

-define(A, {<<"https://a.example/">>, <<"space">>}).

token(N) -> binary:copy(integer_to_binary(N), 64).

now_ms() -> erlang:system_time(millisecond).

outbox_test_() ->
    {foreach,
        fun() ->
            spacepush_test_util:setup_env("outbox"),
            application:set_env(spacepush, delivery_ttl_ms, 3600000),
            application:set_env(spacepush, notification_cooldown_ms, 300000),
            start(spacepush_registry),
            start(spacepush_outbox),
            ok = spacepush_registry:register(token(1), sandbox, [?A])
        end,
        fun(_) ->
            gen_server:stop(spacepush_outbox),
            gen_server:stop(spacepush_registry)
        end,
        [
            fun one_delivery_per_subscriber/0,
            fun newer_state_replaces_pending/0,
            fun late_result_cannot_complete_successor/0,
            fun retry_postpones/0,
            fun in_flight_key_holds_back_successor/0,
            fun expired_is_dropped/0,
            fun survives_restart/0,
            fun sent_topic_waits_for_cooldown/0,
            fun changes_within_cooldown_merge/0
        ]}.

start(Module) ->
    {ok, Pid} = Module:start_link(),
    unlink(Pid).

change(To) -> {?A, <<"A">>, closed, To}.

state(#{payload := Payload}) ->
    #{<<"state">> := State} = json:decode(Payload),
    State.

one_delivery_per_subscriber() ->
    ok = spacepush_registry:register(token(2), production, [?A]),
    ok = spacepush_outbox:enqueue([change(open)]),
    Due = spacepush_outbox:due(now_ms(), [], 10),
    ?assertEqual(lists:sort([token(1), token(2)]), lists:sort([T || #{token := T} <- Due])),
    ?assertEqual([<<"open">>, <<"open">>], [state(D) || D <- Due]).

newer_state_replaces_pending() ->
    ok = spacepush_outbox:enqueue([change(open)]),
    ok = spacepush_outbox:enqueue([change(member)]),
    ?assertMatch([#{token := _}], spacepush_outbox:due(now_ms(), [], 10)),
    [Delivery] = spacepush_outbox:due(now_ms(), [], 10),
    ?assertEqual(<<"member">>, state(Delivery)).

late_result_cannot_complete_successor() ->
    ok = spacepush_outbox:enqueue([change(open)]),
    [#{key := Key, id := OldId}] = spacepush_outbox:due(now_ms(), [], 10),
    ok = spacepush_outbox:enqueue([change(member)]),
    ok = spacepush_outbox:complete(Key, OldId),
    ok = spacepush_outbox:retry(Key, OldId, now_ms() + 60000),
    ?assertMatch([#{attempts := 0}], spacepush_outbox:due(now_ms(), [], 10)).

retry_postpones() ->
    ok = spacepush_outbox:enqueue([change(open)]),
    [#{key := Key, id := Id}] = spacepush_outbox:due(now_ms(), [], 10),
    Later = now_ms() + 10000,
    ok = spacepush_outbox:retry(Key, Id, Later),
    ?assertEqual([], spacepush_outbox:due(now_ms(), [], 10)),
    ?assertMatch([#{attempts := 1}], spacepush_outbox:due(Later, [], 10)),
    ok = spacepush_outbox:complete(Key, Id),
    ?assertEqual([], spacepush_outbox:due(Later, [], 10)).

in_flight_key_holds_back_successor() ->
    ok = spacepush_outbox:enqueue([change(open)]),
    [#{key := Key, id := OpenId}] = spacepush_outbox:due(now_ms(), [], 10),
    ok = spacepush_outbox:enqueue([change(closed)]),
    ?assertEqual([], spacepush_outbox:due(now_ms(), [Key], 10)),
    %% The late result for "open" leaves "closed" in place, which is due once the key is free.
    ok = spacepush_outbox:complete(Key, OpenId),
    ?assertMatch([#{key := Key}], spacepush_outbox:due(now_ms(), [], 10)),
    [Closed] = spacepush_outbox:due(now_ms(), [], 10),
    ?assertEqual(<<"closed">>, state(Closed)).

expired_is_dropped() ->
    application:set_env(spacepush, delivery_ttl_ms, 1000),
    ok = spacepush_outbox:enqueue([change(open)]),
    ?assertEqual([], spacepush_outbox:due(now_ms() + 1000, [], 10)),
    ?assertEqual([], spacepush_outbox:due(now_ms(), [], 10)).

survives_restart() ->
    ok = spacepush_outbox:enqueue([change(open)]),
    gen_server:stop(spacepush_outbox),
    start(spacepush_outbox),
    ?assertMatch([#{token := _}], spacepush_outbox:due(now_ms(), [], 10)).

%% After a notification went out, the next one for the topic waits for the cooldown.
sent_topic_waits_for_cooldown() ->
    ok = spacepush_outbox:enqueue([change(open)]),
    [#{key := Key, id := Id}] = spacepush_outbox:due(now_ms(), [], 10),
    ok = spacepush_outbox:complete(Key, Id),
    ok = spacepush_outbox:enqueue([change(closed)]),
    ?assertEqual([], spacepush_outbox:due(now_ms(), [], 10)),
    ?assertMatch([#{key := Key}], spacepush_outbox:due(now_ms() + 300000, [], 10)).

%% A space flapping open/closed/open within the cooldown yields one pending notification, the latest.
changes_within_cooldown_merge() ->
    ok = spacepush_outbox:enqueue([change(open)]),
    [#{key := Key, id := Id}] = spacepush_outbox:due(now_ms(), [], 10),
    ok = spacepush_outbox:complete(Key, Id),
    ok = spacepush_outbox:enqueue([change(closed)]),
    ok = spacepush_outbox:enqueue([change(open)]),
    [Pending] = spacepush_outbox:due(now_ms() + 300000, [], 10),
    ?assertEqual(<<"open">>, state(Pending)).
