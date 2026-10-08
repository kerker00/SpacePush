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
            fun late_invalid_token_keeps_new_registration/0,
            fun invalid_token_needs_matching_environment/0,
            fun invalid_token_respects_timestamp/0,
            fun capacity/0,
            fun survives_restart/0,
            fun migrates_prototype_records/0,
            fun drops_unknown_records/0,
            fun lookup_returns_current_registration/0,
            fun expires_old_registrations/0
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

drops_unknown_records() ->
    with_raw_records([{token(1), {registration, 99, sandbox, [?A], 1}}, {token(2), garbage}]),
    ?assertEqual([], spacepush_registry:subscribers(?A)),
    ?assertEqual(error, spacepush_registry:lookup(token(1))).

lookup_returns_current_registration() ->
    ?assertEqual(error, spacepush_registry:lookup(token(1))),
    ok = spacepush_registry:register(token(1), production, [?A]),
    ?assertMatch({ok, production, [?A], _}, spacepush_registry:lookup(token(1))).
