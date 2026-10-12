-module(spacepush_apns_tests).
-include_lib("eunit/include/eunit.hrl").

classify_test_() ->
    [
        ?_assertEqual(Expected, spacepush_apns:classify(Status, Reason))
     || {Status, Reason, Expected} <- [
            {200, undefined, delivered},
            {410, <<"Unregistered">>, invalid_token},
            {400, <<"BadDeviceToken">>, invalid_token},
            {400, <<"DeviceTokenNotForTopic">>, invalid_token},
            {400, <<"PayloadEmpty">>, drop},
            {413, <<"PayloadTooLarge">>, drop},
            {403, <<"ExpiredProviderToken">>, renew_token},
            {403, <<"InvalidProviderToken">>, invalid_provider_token},
            {429, <<"TooManyProviderTokenUpdates">>, retry},
            {403, <<"BadCertificate">>, drop},
            {429, <<"TooManyRequests">>, retry},
            {500, <<"InternalServerError">>, retry},
            {503, <<"ServiceUnavailable">>, retry}
        ]
    ].

key_config_test_() ->
    Both = #{sandbox => {"s.p8", <<"S">>}, production => {"p.p8", <<"P">>}},
    [
        ?_assertEqual(Both, spacepush_apns:key_config(Both, "shared.p8", <<"X">>)),
        ?_assertEqual(#{sandbox => {"s.p8", <<"S">>}}, spacepush_apns:key_config(#{sandbox => {"s.p8", <<"S">>}, staging => x}, undefined, undefined)),
        ?_assertEqual(
            #{sandbox => {"shared.p8", <<"X">>}, production => {"shared.p8", <<"X">>}},
            spacepush_apns:key_config(undefined, "shared.p8", <<"X">>)
        ),
        ?_assertEqual(#{}, spacepush_apns:key_config(undefined, undefined, undefined))
    ].

token_usable_test_() ->
    Minute = 60,
    [
        ?_assert(spacepush_apns:token_usable(0, 49 * Minute, false)),
        ?_assertNot(spacepush_apns:token_usable(0, 50 * Minute, false)),
        %% After ExpiredProviderToken a token is replaced only once it is 20 minutes old.
        ?_assert(spacepush_apns:token_usable(0, 19 * Minute, true)),
        ?_assertNot(spacepush_apns:token_usable(0, 20 * Minute, true))
    ].

backoff_grows_and_is_capped_test() ->
    ?assertEqual([5000, 10000, 20000, 40000], [spacepush_apns:backoff_ms(N) || N <- [0, 1, 2, 3]]),
    ?assertEqual(300000, spacepush_apns:backoff_ms(10)),
    ?assertEqual(300000, spacepush_apns:backoff_ms(1000)).

dispatch_test_() ->
    {foreach, fun setup_dispatch/0, fun(_) -> stop_dispatch() end, [
        fun rejects_invalid_persisted_delivery/0,
        fun synchronous_send_failure_is_isolated/0
    ]}.

setup_dispatch() ->
    spacepush_test_util:setup_env("apns-validation"),
    application:set_env(spacepush, registration_ttl_days, 60),
    application:set_env(spacepush, delivery_ttl_ms, 60000),
    application:set_env(spacepush, notification_cooldown_ms, 0),
    [start(M) || M <- [spacepush_registry, spacepush_outbox]].

start(Module) ->
    {ok, Pid} = Module:start_link(),
    unlink(Pid).

stop_dispatch() ->
    [gen_server:stop(M) || M <- [spacepush_outbox, spacepush_registry]].

token(N) ->
    binary:copy(integer_to_binary(N), 64).

topic() ->
    {<<"https://apns-test.example/">>, <<"space">>}.

sender_state(Connection) ->
    #{
        keys => #{sandbox => unused},
        paused => #{},
        connections => #{sandbox => #{pid => Connection, up => true}},
        tokens => #{sandbox => {<<"synthetic-provider-token">>, erlang:system_time(second)}},
        renew => #{},
        topic => <<"test.example">>,
        request_timeout_ms => 15000,
        in_flight => #{},
        max_in_flight => 20
    }.

rejects_invalid_persisted_delivery() ->
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    [#{key := Key} = Delivery] = spacepush_outbox:due(erlang:system_time(millisecond), [], 10),
    replace_delivery(Key, Delivery#{token := <<(token(1))/binary, 10>>}),
    %% This used to reach Gun's request-line parser and crash the sender.
    State = sender_state(self()),
    ?assertEqual({noreply, State}, spacepush_apns:handle_cast(wake, State)),
    ?assertEqual(0, spacepush_outbox:pending()),
    ?assertMatch({ok, sandbox, _, _}, spacepush_registry:lookup(token(1))).

replace_delivery(Key, Delivery) ->
    gen_server:stop(spacepush_outbox),
    {ok, File} = application:get_env(spacepush, outbox_file),
    {ok, Table} = dets:open_file(raw_outbox, [{file, File}, {type, set}]),
    ok = dets:insert(Table, {Key, Delivery}),
    ok = dets:close(Table),
    start(spacepush_outbox).

synchronous_send_failure_is_isolated() ->
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    ok = spacepush_registry:register(token(2), production, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    [#{key := Key} = Delivery] = [
        D || #{token := Token} = D <- spacepush_outbox:due(erlang:system_time(millisecond), [], 10),
             Token =:= token(1)
    ],
    %% A malformed persisted header triggers Gun's synchronous validation.
    replace_delivery(Key, Delivery#{collapse_id := <<"invalid\n">>}),
    %% Production has no key and completes via dry-run without any network.
    State = sender_state(self()),
    ?assertEqual({noreply, State}, spacepush_apns:handle_cast(wake, State)),
    ?assertEqual(1, spacepush_outbox:pending()),
    ?assertEqual([], spacepush_outbox:due(erlang:system_time(millisecond), [], 10)),
    ?assertMatch(
        [#{token := _, attempts := 1}],
        spacepush_outbox:due(erlang:system_time(millisecond) + 5000, [], 10)
    ).
