-module(spacepush_cache_tests).
-include_lib("eunit/include/eunit.hrl").

-define(URL, <<"https://a.example/">>).
-define(SPACE, <<"{\"space\":\"A\",\"state\":{\"open\":true}}">>).

cache_test_() ->
    {foreach,
        fun() ->
            spacepush_test_util:setup_env("cache"),
            application:set_env(spacepush, cache_max_age_ms, 60000),
            application:set_env(spacepush, on_demand_fetch_limit, 20),
            application:set_env(spacepush, on_demand_waiter_limit, 1000),
            application:set_env(spacepush, fetch_timeout_ms, 10000),
            {ok, Pid} = spacepush_cache:start_link(),
            unlink(Pid)
        end,
        fun(_) ->
            gen_server:stop(spacepush_cache),
            application:set_env(spacepush, fetch_timeout_ms, 10000)
        end,
        [
            fun fetches_once_then_serves_cached/0,
            {timeout, 10, fun concurrent_requests_share_one_fetch/0},
            fun serves_stale_entry_when_fetch_fails/0,
            fun rejects_unusable_response/0,
            fun marks_requests_as_watched/0,
            {timeout, 10, fun limits_parallel_fetches/0},
            fun releases_slot_after_worker_crash/0,
            fun expires_hanging_worker/0,
            fun releases_worker_when_caller_dies/0,
            fun bounds_total_waiting_callers/0,
            fun stops_workers_with_cache/0,
            fun crash_keeps_last_good_response/0
        ]}.

fetches_once_then_serves_cached() ->
    spacepush_test_util:fake_responses(#{?URL => {ok, ?SPACE}}),
    ?assertMatch({ok, #{body := ?SPACE, observations := [{_, <<"A">>, open}]}}, spacepush_cache:get(?URL)),
    ?assertMatch({ok, #{body := ?SPACE}}, spacepush_cache:get(?URL)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)).

concurrent_requests_share_one_fetch() ->
    spacepush_test_util:fake_responses(#{?URL => {delay, 200, {ok, ?SPACE}}}),
    Self = self(),
    [spawn(fun() -> Self ! {done, spacepush_cache:get(?URL)} end) || _ <- lists:seq(1, 5)],
    Results = [receive {done, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 5)],
    ?assert(lists:all(fun(R) -> element(1, R) =:= ok end, Results)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)).

serves_stale_entry_when_fetch_fails() ->
    spacepush_cache:store(?URL, ?SPACE, []),
    application:set_env(spacepush, cache_max_age_ms, 0),
    spacepush_test_util:fake_responses(#{?URL => {error, timeout}}),
    ?assertMatch({ok, #{body := ?SPACE}}, spacepush_cache:get(?URL)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)).

rejects_unusable_response() ->
    spacepush_test_util:fake_responses(#{?URL => {ok, <<"<html>">>}}),
    ?assertEqual({error, invalid_response}, spacepush_cache:get(?URL)),
    ?assertEqual(none, spacepush_cache:lookup(?URL)).

marks_requests_as_watched() ->
    Before = erlang:system_time(millisecond),
    spacepush_test_util:fake_responses(#{?URL => {ok, ?SPACE}}),
    {ok, _} = spacepush_cache:get(?URL),
    ?assertEqual([?URL], spacepush_cache:watched(Before)),
    ?assertEqual([], spacepush_cache:watched(erlang:system_time(millisecond) + 1)).

limits_parallel_fetches() ->
    application:set_env(spacepush, on_demand_fetch_limit, 1),
    Other = <<"https://b.example/">>,
    spacepush_test_util:fake_responses(#{?URL => {delay, 300, {ok, ?SPACE}}, Other => {ok, ?SPACE}}),
    Self = self(),
    spawn(fun() -> Self ! {slow, spacepush_cache:get(?URL)} end),
    timer:sleep(50),
    ?assertEqual({error, busy}, spacepush_cache:get(Other)),
    ?assertMatch({slow, {ok, _}}, receive Msg -> Msg after 5000 -> timeout end).

releases_slot_after_worker_crash() ->
    application:set_env(spacepush, on_demand_fetch_limit, 1),
    {_Caller, Transport} = start_waiting_fetch(),
    TransportRef = monitor(process, Transport),
    #{waiting := #{?URL := #{pid := Worker}}} = sys:get_state(spacepush_cache),
    exit(Worker, kill),
    ?assertEqual({error, {crashed, killed}}, fetch_result()),
    assert_stopped(TransportRef, Transport),
    ?assertEqual(#{}, maps:get(waiting, sys:get_state(spacepush_cache))),
    assert_recovered().

expires_hanging_worker() ->
    application:set_env(spacepush, fetch_timeout_ms, 50),
    {_Caller, Transport} = start_waiting_fetch(),
    TransportRef = monitor(process, Transport),
    ?assertEqual({error, timeout}, fetch_result()),
    assert_stopped(TransportRef, Transport),
    ?assertEqual(none, spacepush_cache:lookup(?URL)),
    assert_recovered().

releases_worker_when_caller_dies() ->
    {Caller, Transport} = start_waiting_fetch(),
    TransportRef = monitor(process, Transport),
    exit(Caller, kill),
    assert_stopped(TransportRef, Transport),
    ?assertEqual(#{}, maps:get(waiting, sys:get_state(spacepush_cache))),
    ?assertEqual(#{}, maps:get(callers, sys:get_state(spacepush_cache))),
    assert_recovered().

bounds_total_waiting_callers() ->
    application:set_env(spacepush, on_demand_waiter_limit, 2),
    {_Caller, Transport} = start_waiting_fetch(),
    Self = self(),
    spawn(fun() -> Self ! {fetch_result, spacepush_cache:get(?URL)} end),
    wait_for_callers(2, erlang:monotonic_time(millisecond) + 1000),
    ?assertEqual({error, busy}, spacepush_cache:get(?URL)),
    ?assertEqual({error, busy}, spacepush_cache:get(<<"https://b.example/">>)),
    Transport ! continue,
    ?assertMatch({ok, _}, fetch_result()),
    ?assertMatch({ok, _}, fetch_result()),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)),
    ?assertEqual(#{}, maps:get(callers, sys:get_state(spacepush_cache))).

stops_workers_with_cache() ->
    {Caller, Transport} = start_waiting_fetch(),
    TransportRef = monitor(process, Transport),
    gen_server:stop(spacepush_cache),
    assert_stopped(TransportRef, Transport),
    %% Restore the fixture process for the foreach cleanup.
    {ok, Pid} = spacepush_cache:start_link(),
    unlink(Pid),
    Ref = monitor(process, Caller),
    assert_stopped(Ref, Caller).

crash_keeps_last_good_response() ->
    spacepush_cache:store(?URL, ?SPACE, []),
    application:set_env(spacepush, cache_max_age_ms, 0),
    spacepush_test_util:fake_responses(#{?URL => {crash, test_transport_failure}}),
    ?assertMatch({ok, #{body := ?SPACE}}, spacepush_cache:get(?URL)),
    ?assertEqual(#{}, maps:get(waiting, sys:get_state(spacepush_cache))).

start_waiting_fetch() ->
    spacepush_test_util:fake_responses(#{?URL => {notify, self(), {wait, {ok, ?SPACE}}}}),
    Self = self(),
    Caller = spawn(fun() -> Self ! {fetch_result, spacepush_cache:get(?URL)} end),
    Transport = receive {fetch_worker, Pid} -> Pid after 1000 -> error(worker_not_started) end,
    {Caller, Transport}.

fetch_result() ->
    receive {fetch_result, Result} -> Result after 1000 -> error(fetch_did_not_end) end.

assert_stopped(Ref, Pid) ->
    receive {'DOWN', Ref, process, Pid, _Reason} -> ok after 1000 -> error(worker_still_alive) end.

assert_recovered() ->
    spacepush_test_util:fake_responses(#{?URL => {ok, ?SPACE}}),
    ?assertMatch({ok, #{body := ?SPACE}}, spacepush_cache:get(?URL)).

wait_for_callers(N, Deadline) ->
    #{callers := Callers} = sys:get_state(spacepush_cache),
    case map_size(Callers) of
        N -> ok;
        _ ->
            ?assert(erlang:monotonic_time(millisecond) < Deadline),
            timer:sleep(1),
            wait_for_callers(N, Deadline)
    end.
