-module(spacepush_registry_tests).
-include_lib("eunit/include/eunit.hrl").

-define(A, {<<"https://a.example/">>, <<"space">>}).
-define(B, {<<"https://b.example/">>, <<"space">>}).

token(N) -> binary:copy(integer_to_binary(N), 64).

registry_test_() ->
    {foreach,
        fun() ->
            spacepush_test_util:setup_env("registry"),
            application:set_env(spacepush, max_registrations, 3),
            application:set_env(spacepush, registration_ttl_days, 60),
            start()
        end,
        fun(_) -> stop() end,
        [
            fun lookup/0,
            fun reregister_replaces_topics/0,
            fun renewal_remains_visible_during_publication/0,
            fun topic_changes_publish_with_the_registration/0,
            fun late_invalid_token_keeps_new_registration/0,
            fun invalid_token_needs_matching_environment/0,
            fun invalid_token_respects_timestamp/0,
            fun capacity/0,
            fun survives_restart/0,
            fun migrates_prototype_records/0,
            fun migrates_format_1_records/0,
            fun keeps_platform/0,
            fun drops_unknown_records/0,
            fun rejects_invalid_registration/0,
            fun drops_invalid_persisted_tokens_and_rooms/0,
            fun lookup_returns_current_registration/0,
            fun subscribed_endpoints_are_unique/0,
            fun expires_old_registrations/0,
            fun empty_topics_never_consume_capacity/0,
            fun empty_topics_remove_existing_registration/0,
            fun empty_topics_work_at_full_capacity/0,
            fun drops_persisted_empty_registrations/0,
            fun validates_all_persisted_registration_fields/0,
            fun corrupt_dets_file_has_explicit_start_error/0
        ]}.

start() ->
    {ok, Pid} = spacepush_registry:start_link(),
    unlink(Pid).

stop() ->
    gen_server:stop(spacepush_registry).

version(Token, Topic) ->
    [Version] = [V || {T, _Env, V} <- spacepush_registry:subscribers(Topic), T =:= Token],
    Version.

lookup() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A, ?B]),
    ?assertMatch([{_, sandbox, _}], spacepush_registry:subscribers(?A)),
    ?assertMatch([{_, sandbox, _}], spacepush_registry:subscribers(?B)).

reregister_replaces_topics() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    ok = spacepush_registry:register(token(1), production, [?B]),
    ?assertEqual([], spacepush_registry:subscribers(?A)),
    ?assertMatch([{_, production, _}], spacepush_registry:subscribers(?B)).

with_paused_renewal(Environment, Topics, Before, After) ->
    Parent = self(),
    Registry = whereis(spacepush_registry),
    application:set_env(spacepush, registry_publication_hook, fun(Stage) ->
        Parent ! {publication, Stage, self()},
        receive continue_publication -> ok after 2000 -> error(publication_not_resumed) end
    end),
    {Worker, Monitor} = spawn_monitor(fun() ->
        Result = spacepush_registry:register(token(1), Environment, Topics),
        Parent ! {renewal_result, self(), Result}
    end),
    try
        receive {publication, indexed, Registry} -> ok after 2000 -> error(index_not_prepared) end,
        Before(),
        Registry ! continue_publication,
        receive {publication, published, Registry} -> ok after 2000 -> error(registration_not_published) end,
        After(),
        Registry ! continue_publication,
        receive {renewal_result, Worker, Result} -> ?assertEqual(ok, Result) after 2000 -> error(renewal_not_finished) end,
        receive {'DOWN', Monitor, process, Worker, Reason} -> ?assertEqual(normal, Reason) after 2000 -> error(worker_not_finished) end
    after
        application:unset_env(spacepush, registry_publication_hook),
        Registry ! continue_publication,
        Registry ! continue_publication
    end.

renewal_remains_visible_during_publication() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    {ok, sandbox, [?A], OldVersion} = spacepush_registry:lookup(token(1)),
    {ok, Outbox} = spacepush_outbox:start_link(),
    unlink(Outbox),
    try
        ok = spacepush_outbox:enqueue([{?A, <<"A">>, closed, open}]),
        with_paused_renewal(sandbox, [?A], fun() ->
            ?assertEqual({ok, sandbox, [?A], OldVersion}, spacepush_registry:lookup(token(1))),
            ?assertEqual([{token(1), sandbox, OldVersion}], spacepush_registry:subscribers(?A)),
            %% A confirmed change during publication must still fan out.
            ok = spacepush_outbox:enqueue([{?A, <<"A">>, open, closed}]),
            [#{payload := Payload}] = spacepush_outbox:due(erlang:system_time(millisecond), [], 10),
            ?assertMatch(#{<<"state">> := <<"closed">>}, json:decode(Payload))
        end, fun() ->
            {ok, sandbox, [?A], NewVersion} = spacepush_registry:lookup(token(1)),
            ?assert(NewVersion > OldVersion),
            ?assertEqual([{token(1), sandbox, NewVersion}], spacepush_registry:subscribers(?A)),
            ?assertEqual(1, spacepush_outbox:pending())
        end)
    after gen_server:stop(spacepush_outbox) end,
    stop(),
    start(),
    ?assertMatch({ok, sandbox, [?A], _}, spacepush_registry:lookup(token(1))).

topic_changes_publish_with_the_registration() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    with_paused_renewal(production, [?B, ?B], fun() ->
        ?assertMatch({ok, sandbox, [?A], _}, spacepush_registry:lookup(token(1))),
        ?assertMatch([{_, sandbox, _}], spacepush_registry:subscribers(?A)),
        ?assertEqual([], spacepush_registry:subscribers(?B)),
        ?assertEqual([element(1, ?A)], spacepush_registry:subscribed_endpoints()),
        ?assertMatch(#{<<"subscriptions">> := [#{<<"endpoint">> := <<"https://a.example/">>, <<"devices">> := 1}]}, spacepush_registry:summary())
    end, fun() ->
        ?assertMatch({ok, production, [?B, ?B], _}, spacepush_registry:lookup(token(1))),
        ?assertEqual([], spacepush_registry:subscribers(?A)),
        ?assertMatch([{_, production, _}], spacepush_registry:subscribers(?B)),
        ?assertEqual([element(1, ?B)], spacepush_registry:subscribed_endpoints()),
        ?assertMatch(#{<<"subscriptions">> := [#{<<"endpoint">> := <<"https://b.example/">>, <<"devices">> := 1}]}, spacepush_registry:summary())
    end),
    ?assertEqual([], ets:lookup(spacepush_topic_index, ?A)).

late_invalid_token_keeps_new_registration() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    Old = version(token(1), ?A),
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    New = version(token(1), ?A),
    ?assert(New > Old),
    ok = spacepush_registry:unregister_if(token(1), sandbox, Old, undefined),
    ?assertEqual(New, version(token(1), ?A)),
    ok = spacepush_registry:unregister_if(token(1), sandbox, New, undefined),
    ?assertEqual([], spacepush_registry:subscribers(?A)).

invalid_token_needs_matching_environment() ->
    ok = spacepush_registry:register(token(1), production, [?A]),
    Version = version(token(1), ?A),
    ok = spacepush_registry:unregister_if(token(1), sandbox, Version, undefined),
    ?assertMatch([_], spacepush_registry:subscribers(?A)).

invalid_token_respects_timestamp() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    Version = version(token(1), ?A),
    ok = spacepush_registry:unregister_if(token(1), sandbox, Version, Version - 1),
    ?assertMatch([_], spacepush_registry:subscribers(?A)),
    ok = spacepush_registry:unregister_if(token(1), sandbox, Version, Version + 1),
    ?assertEqual([], spacepush_registry:subscribers(?A)).

capacity() ->
    [ok = spacepush_registry:register(token(N), sandbox, [?A]) || N <- [1, 2, 3]],
    ?assertEqual({error, full}, spacepush_registry:register(token(4), sandbox, [?A])),
    ?assertEqual(ok, spacepush_registry:register(token(1), sandbox, [?B])),
    ok = spacepush_registry:unregister(token(2)),
    ?assertEqual(ok, spacepush_registry:register(token(4), sandbox, [?A])).

survives_restart() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    stop(),
    start(),
    ?assertMatch([{_, sandbox, _}], spacepush_registry:subscribers(?A)).

expires_old_registrations() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A]),
    stop(),
    timer:sleep(5),
    application:set_env(spacepush, registration_ttl_days, 0),
    start(),
    ?assertEqual([], spacepush_registry:subscribers(?A)).

%% Writes records straight into the DETS file while the registry is stopped.
with_raw_records(Records) ->
    stop(),
    {ok, File} = application:get_env(spacepush, registry_file),
    {ok, Table} = dets:open_file(raw_registry, [{file, File}, {type, set}]),
    ok = dets:insert(Table, Records),
    ok = dets:close(Table),
    start().

migrates_prototype_records() ->
    Seconds = erlang:system_time(second),
    with_raw_records([{token(1), sandbox, [?A], Seconds}]),
    ?assertEqual([{token(1), sandbox, Seconds * 1000}], spacepush_registry:subscribers(?A)),
    stop(),
    start(),
    ?assertEqual([{token(1), sandbox, Seconds * 1000}], spacepush_registry:subscribers(?A)).

migrates_format_1_records() ->
    Version = erlang:system_time(millisecond),
    with_raw_records([{token(1), {registration, 1, production, [?A], Version}}]),
    ?assertEqual([{token(1), production, Version}], spacepush_registry:subscribers(?A)),
    ?assertMatch(#{<<"by_platform">> := #{<<"unknown">> := 1}}, spacepush_registry:summary()),
    stop(),
    {ok, File} = application:get_env(spacepush, registry_file),
    {ok, Table} = dets:open_file(raw_registry, [{file, File}, {type, set}]),
    ?assertEqual([{token(1), {registration, 2, production, [?A], Version, <<"unknown">>}}], dets:lookup(Table, token(1))),
    ok = dets:close(Table),
    start().

keeps_platform() ->
    ok = spacepush_registry:register(token(1), production, [?A], <<"macos">>),
    stop(),
    start(),
    ?assertMatch(#{<<"total">> := 1, <<"by_platform">> := #{<<"macos">> := 1}}, spacepush_registry:summary()).

drops_unknown_records() ->
    with_raw_records([{token(1), {registration, 99, sandbox, [?A], 1}}, {token(2), garbage}]),
    ?assertEqual([], spacepush_registry:subscribers(?A)),
    ?assertEqual(error, spacepush_registry:lookup(token(1))).

rejects_invalid_registration() ->
    ?assertEqual({error, invalid_registration}, spacepush_registry:register(<<(token(1))/binary, 10>>, sandbox, [?A])),
    ?assertEqual(
        {error, invalid_registration},
        spacepush_registry:register(token(1), sandbox, [{element(1, ?A), <<"space\n">>}])
    ),
    ?assertMatch(#{<<"total">> := 0}, spacepush_registry:summary()).

drops_invalid_persisted_tokens_and_rooms() ->
    Version = erlang:system_time(millisecond),
    BadToken = <<(token(2))/binary, 10>>,
    OldBadToken = <<(token(3))/binary, 0>>,
    BadRoom = {element(1, ?A), <<"space\n">>},
    with_raw_records([
        {token(1), {registration, 2, sandbox, [?A], Version, <<"ios">>}},
        {BadToken, {registration, 2, production, [?A], Version, <<"ios">>}},
        {token(4), {registration, 1, sandbox, [BadRoom], Version}},
        {OldBadToken, production, [?A], Version div 1000}
    ]),
    ?assertEqual([{token(1), sandbox, Version}], spacepush_registry:subscribers(?A)),
    ?assertEqual(error, spacepush_registry:lookup(BadToken)),
    ?assertEqual(error, spacepush_registry:lookup(OldBadToken)),
    ?assertEqual(error, spacepush_registry:lookup(token(4))),
    stop(),
    {ok, File} = application:get_env(spacepush, registry_file),
    {ok, Table} = dets:open_file(raw_registry, [{file, File}, {type, set}]),
    ?assertEqual(1, dets:info(Table, size)),
    ok = dets:close(Table),
    start(),
    ?assertEqual([{token(1), sandbox, Version}], spacepush_registry:subscribers(?A)).

lookup_returns_current_registration() ->
    ?assertEqual(error, spacepush_registry:lookup(token(1))),
    ok = spacepush_registry:register(token(1), production, [?A]),
    ?assertMatch({ok, production, [?A], _}, spacepush_registry:lookup(token(1))).

subscribed_endpoints_are_unique() ->
    Room = {<<"https://a.example/">>, <<"radstelle">>},
    ok = spacepush_registry:register(token(1), sandbox, [?A, Room]),
    ok = spacepush_registry:register(token(2), sandbox, [?A, ?B]),
    ?assertEqual([<<"https://a.example/">>, <<"https://b.example/">>], spacepush_registry:subscribed_endpoints()),
    ok = spacepush_registry:unregister(token(2)),
    ?assertEqual([<<"https://a.example/">>], spacepush_registry:subscribed_endpoints()).

empty_topics_never_consume_capacity() ->
    [ok = spacepush_registry:register(token(N), sandbox, []) || N <- [1, 2, 3, 4]],
    ?assertMatch(#{<<"total">> := 0}, spacepush_registry:summary()),
    ?assertEqual(error, spacepush_registry:lookup(token(1))),
    ?assertEqual(ok, spacepush_registry:register(token(1), sandbox, [?A])),
    stop(),
    start(),
    ?assertMatch(#{<<"total">> := 1}, spacepush_registry:summary()).

empty_topics_remove_existing_registration() ->
    ok = spacepush_registry:register(token(1), sandbox, [?A, ?B]),
    ok = spacepush_registry:register(token(1), production, [], <<"ios">>),
    ?assertEqual(error, spacepush_registry:lookup(token(1))),
    ?assertEqual([], spacepush_registry:subscribers(?A)),
    ?assertEqual([], spacepush_registry:subscribers(?B)),
    ?assertEqual([], spacepush_registry:subscribed_endpoints()),
    stop(),
    start(),
    ?assertEqual(error, spacepush_registry:lookup(token(1))),
    ?assertMatch(#{<<"total">> := 0}, spacepush_registry:summary()).

empty_topics_work_at_full_capacity() ->
    [ok = spacepush_registry:register(token(N), sandbox, [?A]) || N <- [1, 2, 3]],
    ?assertEqual({error, full}, spacepush_registry:register(token(4), sandbox, [?A])),
    ?assertEqual(ok, spacepush_registry:register(token(4), sandbox, [])),
    ?assertMatch(#{<<"total">> := 3}, spacepush_registry:summary()),
    ?assertEqual(ok, spacepush_registry:register(token(1), sandbox, [])),
    ?assertEqual(ok, spacepush_registry:register(token(4), sandbox, [?A])),
    ?assertMatch(#{<<"total">> := 3}, spacepush_registry:summary()).

drops_persisted_empty_registrations() ->
    Version = erlang:system_time(millisecond),
    with_raw_records([
        {token(1), {registration, 2, sandbox, [?A], Version, <<"ios">>}},
        {token(2), {registration, 2, sandbox, [], Version, <<"ios">>}},
        {token(3), {registration, 1, production, [], Version}},
        {token(4), production, [], Version div 1000}
    ]),
    ?assertEqual([{token(1), sandbox, Version}], spacepush_registry:subscribers(?A)),
    ?assertMatch(#{<<"total">> := 1}, spacepush_registry:summary()),
    stop(),
    {ok, File} = application:get_env(spacepush, registry_file),
    {ok, Table} = dets:open_file(raw_registry, [{file, File}, {type, set}]),
    ?assertEqual(1, dets:info(Table, size)),
    ok = dets:close(Table),
    start(),
    ?assertMatch(#{<<"total">> := 1}, spacepush_registry:summary()).

validates_all_persisted_registration_fields() ->
    Version = erlang:system_time(millisecond),
    with_raw_records([
        {token(1), {registration, 2, sandbox, [?A], Version, <<"ios">>}},
        {token(2)},
        {token(3), {registration, 2, sandbox, [{<<255>>, <<"space">>}], Version, <<"ios">>}},
        {token(4), {registration, 2, sandbox, [?A], 1 bsl 128, <<"ios">>}},
        {token(5), {registration, 2, sandbox, [?A], Version, #{}}},
        {token(6), {registration, 2, sandbox, [?A], Version, <<"unrecognized">>}}
    ]),
    ?assertEqual([{token(1), sandbox, Version}], spacepush_registry:subscribers(?A)),
    stop(), start(),
    ?assertMatch(#{<<"total">> := 1}, spacepush_registry:summary()).

corrupt_dets_file_has_explicit_start_error() ->
    stop(),
    {ok, File} = application:get_env(spacepush, registry_file),
    ok = file:write_file(File, <<"not a DETS file">>),
    try
        {Pid, Ref} = spawn_monitor(fun() -> spacepush_registry:init([]) end),
        receive {'DOWN', Ref, process, Pid, Reason} ->
            ?assertMatch({{state_table_open_failed, File, _}, _}, Reason)
        after 2000 -> error(corrupt_registry_was_not_rejected) end
    after ok = file:delete(File), start() end.
