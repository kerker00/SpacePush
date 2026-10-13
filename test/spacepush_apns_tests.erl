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
        fun synchronous_send_failure_is_isolated/0,
        fun down_environment_does_not_starve_ready_environment/0,
        fun paused_environment_does_not_starve_ready_environment/0,
        fun both_environments_receive_capacity/0,
        fun one_slot_alternates_between_ready_environments/0,
        fun selects_the_current_registration_environment/0,
        fun server_errors_wait_fifteen_minutes/0,
        fun rate_limits_keep_exponential_backoff/0,
        fun short_lived_server_errors_expire_without_early_retry/0,
        fun sender_holds_in_flight_successor_until_cooldown/0,
        fun successful_response_confirms_current_registration/0,
        fun failed_response_does_not_confirm_registration/0,
        fun late_success_does_not_confirm_renewed_registration/0,
        fun dry_run_does_not_confirm_registration/0
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
    %% Startup now rejects this record; inject runtime corruption to retain
    %% coverage of the synchronous Gun failure boundary independently.
    sys:replace_state(spacepush_outbox, fun(OutboxState) ->
        true = ets:insert(spacepush_outbox_entries, {Key, Delivery#{collapse_id := <<"invalid\n">>}}),
        OutboxState
    end),
    %% Production has no key and completes via dry-run without any network.
    State = sender_state(self()),
    {noreply, Updated} = spacepush_apns:handle_cast(wake, State),
    ?assertEqual(#{}, maps:get(in_flight, Updated)),
    ?assertEqual(1, spacepush_outbox:pending()),
    ?assertEqual([], spacepush_outbox:due(erlang:system_time(millisecond), [], 10)),
    ?assertMatch(
        [#{token := _, attempts := 1}],
        spacepush_outbox:due(erlang:system_time(millisecond) + 5000, [], 10)
    ).

fair_sender(Max) ->
    Now = erlang:system_time(second),
    (sender_state(self()))#{
        keys := #{sandbox => unused, production => unused},
        connections := #{sandbox => #{pid => self(), up => true}, production => #{pid => self(), up => true}},
        tokens := #{sandbox => {<<"synthetic-provider-token">>, Now}, production => {<<"synthetic-provider-token">>, Now}},
        max_in_flight := Max
    }.

enqueue_pair(FirstEnvironment, SecondEnvironment) ->
    ok = spacepush_registry:register(token(1), FirstEnvironment, [topic()]),
    ok = spacepush_registry:register(token(2), SecondEnvironment, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    [Older] = [D || #{token := T} = D <- spacepush_outbox:due(erlang:system_time(millisecond), [], 10), T =:= token(1)],
    replace_delivery(maps:get(key, Older), Older#{not_before := erlang:system_time(millisecond) - 1000}).

finish_only_request(State) ->
    [Ref] = maps:keys(maps:get(in_flight, State)),
    {noreply, Updated} = spacepush_apns:handle_info({gun_response, self(), Ref, fin, 200, []}, State),
    Updated.

down_environment_does_not_starve_ready_environment() ->
    lists:foreach(fun({Down, Ready}) ->
        enqueue_pair(Down, Ready),
        State = fair_sender(1),
        Connections = maps:get(connections, State),
        DownConnection = (maps:get(Down, Connections))#{up := false},
        {noreply, Sent} = spacepush_apns:handle_cast(wake, State#{connections := Connections#{Down := DownConnection}}),
        [#{delivery := #{environment := Ready}}] = maps:values(maps:get(in_flight, Sent)),
        Completed = finish_only_request(Sent),
        ?assertEqual(1, spacepush_outbox:pending()),
        {noreply, Again} = spacepush_apns:handle_cast(wake, Completed),
        ?assertEqual(#{}, maps:get(in_flight, Again)),
        ?assertEqual(1, spacepush_outbox:pending())
    end, [{sandbox, production}, {production, sandbox}]).

paused_environment_does_not_starve_ready_environment() ->
    enqueue_pair(sandbox, production),
    State = (fair_sender(1))#{paused := #{sandbox => erlang:system_time(millisecond) + 60000}},
    {noreply, Sent} = spacepush_apns:handle_cast(wake, State),
    [#{delivery := #{environment := production}}] = maps:values(maps:get(in_flight, Sent)),
    Completed = finish_only_request(Sent),
    ?assertEqual(1, spacepush_outbox:pending()),
    {noreply, Resumed} = spacepush_apns:handle_cast(wake, Completed#{paused := #{}}),
    [#{delivery := #{environment := sandbox}}] = maps:values(maps:get(in_flight, Resumed)),
    finish_only_request(Resumed),
    ?assertEqual(0, spacepush_outbox:pending()).

both_environments_receive_capacity() ->
    application:set_env(spacepush, max_registrations, 30),
    [ok = spacepush_registry:register(token(N), sandbox, [topic()]) || N <- lists:seq(1, 20)],
    ok = spacepush_registry:register(token(21), production, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    {noreply, Sent} = spacepush_apns:handle_cast(wake, fair_sender(20)),
    Environments = [E || #{delivery := #{environment := E}} <- maps:values(maps:get(in_flight, Sent))],
    ?assertEqual(10, length([E || E <- Environments, E =:= sandbox])),
    ?assertEqual(1, length([E || E <- Environments, E =:= production])).

one_slot_alternates_between_ready_environments() ->
    enqueue_pair(sandbox, production),
    {noreply, First} = spacepush_apns:handle_cast(wake, fair_sender(1)),
    [#{delivery := #{environment := sandbox}}] = maps:values(maps:get(in_flight, First)),
    Completed = finish_only_request(First),
    %% Sandbox gets new work too; production still receives the next slot.
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, open, closed}]),
    {noreply, Second} = spacepush_apns:handle_cast(wake, Completed),
    [#{delivery := #{environment := production}}] = maps:values(maps:get(in_flight, Second)).

selects_the_current_registration_environment() ->
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    ok = spacepush_registry:register(token(1), production, [topic()]),
    State = fair_sender(1),
    Connections = maps:get(connections, State),
    {noreply, Sent} = spacepush_apns:handle_cast(wake, State#{connections := Connections#{sandbox := #{pid => self(), up => false}}}),
    [#{delivery := #{environment := production}}] = maps:values(maps:get(in_flight, Sent)),
    finish_only_request(Sent),
    ?assertEqual(0, spacepush_outbox:pending()).

send_and_respond(Status) ->
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    {noreply, Sent} = spacepush_apns:handle_cast(wake, sender_state(self())),
    [Ref] = maps:keys(maps:get(in_flight, Sent)),
    {noreply, Completed} = spacepush_apns:handle_info({gun_response, self(), Ref, fin, Status, []}, Sent),
    ?assertEqual(#{}, maps:get(in_flight, Completed)).

confirmed() ->
    #{<<"by_confirmation">> := Counts} = spacepush_registry:summary(),
    maps:get(<<"accepted_by_apns">>, Counts, 0).

successful_response_confirms_current_registration() ->
    send_and_respond(200),
    ?assertEqual(1, confirmed()),
    ?assertEqual(0, spacepush_outbox:pending()),
    gen_server:stop(spacepush_registry),
    start(spacepush_registry),
    ?assertEqual(1, confirmed()).

failed_response_does_not_confirm_registration() ->
    lists:foreach(fun(Status) ->
        send_and_respond(Status),
        ?assertEqual(0, confirmed()),
        %% Expire this case's retry before dispatching the next independent case.
        ?assertEqual([], spacepush_outbox:due(erlang:system_time(millisecond) + 3600000, [], 10))
    end, [429, 500, 413]).

late_success_does_not_confirm_renewed_registration() ->
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    {noreply, Sent} = spacepush_apns:handle_cast(wake, sender_state(self())),
    [Ref] = maps:keys(maps:get(in_flight, Sent)),
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    {noreply, _} = spacepush_apns:handle_info({gun_response, self(), Ref, fin, 200, []}, Sent),
    ?assertEqual(0, confirmed()).

dry_run_does_not_confirm_registration() ->
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    {noreply, _} = spacepush_apns:handle_cast(wake, (sender_state(self()))#{keys := #{}}),
    ?assertEqual(0, spacepush_outbox:pending()),
    ?assertEqual(0, confirmed()).

server_errors_wait_fifteen_minutes() ->
    application:set_env(spacepush, delivery_ttl_ms, 3600000),
    lists:foreach(fun(Status) ->
        Before = erlang:system_time(millisecond),
        send_and_respond(Status),
        ?assertEqual([], spacepush_outbox:due(Before + 899999, [], 10)),
        [#{key := Key, id := Id, attempts := 1}] = spacepush_outbox:due(erlang:system_time(millisecond) + 900000, [], 10),
        ok = spacepush_outbox:complete(Key, Id)
    end, [500, 503]).

rate_limits_keep_exponential_backoff() ->
    Before = erlang:system_time(millisecond),
    send_and_respond(429),
    ?assertEqual([], spacepush_outbox:due(Before + 4999, [], 10)),
    ?assertMatch([#{attempts := 1}], spacepush_outbox:due(erlang:system_time(millisecond) + 5000, [], 10)).

short_lived_server_errors_expire_without_early_retry() ->
    application:set_env(spacepush, delivery_ttl_ms, 60000),
    Before = erlang:system_time(millisecond),
    send_and_respond(503),
    ?assertEqual([], spacepush_outbox:due(Before + 59999, [], 10)),
    ?assertEqual([], spacepush_outbox:due(erlang:system_time(millisecond) + 60000, [], 10)),
    ?assertEqual(0, spacepush_outbox:pending()).

sender_holds_in_flight_successor_until_cooldown() ->
    application:set_env(spacepush, notification_cooldown_ms, 300000),
    ok = spacepush_registry:register(token(1), sandbox, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, closed, open}]),
    {noreply, Sent} = spacepush_apns:handle_cast(wake, sender_state(self())),
    [Ref] = maps:keys(maps:get(in_flight, Sent)),
    ok = spacepush_outbox:enqueue([{topic(), <<"Test">>, open, closed}]),
    {noreply, Completed} = spacepush_apns:handle_info({gun_response, self(), Ref, fin, 200, []}, Sent),
    {noreply, Waiting} = spacepush_apns:handle_cast(wake, Completed),
    ?assertEqual(#{}, maps:get(in_flight, Waiting)),
    ?assertEqual(1, spacepush_outbox:pending()),
    ?assertEqual([], spacepush_outbox:due(erlang:system_time(millisecond), [], 10)),
    ?assertMatch([#{payload := _}], spacepush_outbox:due(erlang:system_time(millisecond) + 300000, [], 10)),
    ?assertEqual({noreply, Waiting}, spacepush_apns:handle_info({gun_response, self(), Ref, fin, 200, []}, Waiting)).
