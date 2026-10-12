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
            %% Lifecycle cases allow immediate recovery; admission cases override this.
            application:set_env(spacepush, fetch_min_interval_ms, 0),
            application:set_env(spacepush, fetch_backoff_max_ms, 1800000),
            {ok, Pid} = spacepush_cache:start_link(),
            unlink(Pid)
        end,
        fun(_) ->
            gen_server:stop(spacepush_cache),
            application:set_env(spacepush, fetch_timeout_ms, 10000),
            application:set_env(spacepush, fetch_min_interval_ms, 60000),
            application:set_env(spacepush, fetch_backoff_max_ms, 1800000)
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
            fun rejects_completion_processed_after_deadline/0,
            fun releases_worker_when_caller_dies/0,
            fun bounds_total_waiting_callers/0,
            fun stops_workers_with_cache/0,
            fun crash_keeps_last_good_response/0,
            fun sequential_reads_respect_failure_backoff/0,
            fun failure_without_cache_does_not_retry_immediately/0,
            fun successful_attempts_keep_minimum_interval/0,
            fun failure_backoff_grows_caps_and_resets/0,
            fun shared_failure_only_falls_back_for_readers/0,
            fun unusable_mainframe_keeps_last_good_result/0
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

rejects_completion_processed_after_deadline() ->
    application:set_env(spacepush, fetch_timeout_ms, 200),
    {_Caller, Transport} = start_waiting_fetch(),
    #{waiting := #{?URL := #{ref := Ref, deadline := Deadline}}} = sys:get_state(spacepush_cache),
    Cache = whereis(spacepush_cache),
    sys:suspend(Cache),
    try
        Transport ! continue,
        wait_for_queued_result(Cache, Ref, Deadline),
        timer:sleep(max(0, Deadline - erlang:monotonic_time(millisecond)) + 10)
    after sys:resume(Cache) end,
    ?assertEqual({error, timeout}, fetch_result()),
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
    start_waiting_fetch({ok, ?SPACE}).

start_waiting_fetch(Result) ->
    spacepush_test_util:fake_responses(#{?URL => {notify, self(), {wait, Result}}}),
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

wait_for_queued_result(Cache, Ref, Deadline) ->
    {messages, Messages} = process_info(Cache, messages),
    Found = lists:any(fun
        ({'DOWN', R, process, _Pid, {fetched, {ok, _, _}}}) -> R =:= Ref;
        (_) -> false
    end, Messages),
    case Found of
        true -> ok;
        false ->
            ?assert(erlang:monotonic_time(millisecond) < Deadline),
            timer:sleep(1),
            wait_for_queued_result(Cache, Ref, Deadline)
    end.

sequential_reads_respect_failure_backoff() ->
    application:set_env(spacepush, fetch_min_interval_ms, 60000),
    application:set_env(spacepush, cache_max_age_ms, 0),
    spacepush_cache:store(?URL, ?SPACE, []),
    Stale = spacepush_cache:lookup(?URL),
    spacepush_test_util:fake_responses(#{?URL => {error, timeout}}),
    [?assertEqual({ok, Stale}, spacepush_cache:get(?URL)) || _ <- lists:seq(1, 10)],
    ?assertEqual({error, backoff}, spacepush_cache:refresh(?URL)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)),
    ?assertEqual(Stale, spacepush_cache:lookup(?URL)).

failure_without_cache_does_not_retry_immediately() ->
    application:set_env(spacepush, fetch_min_interval_ms, 60000),
    Other = <<"https://b.example/">>,
    spacepush_test_util:fake_responses(#{?URL => {error, timeout}, Other => {ok, ?SPACE}}),
    ?assertEqual({error, timeout}, spacepush_cache:get(?URL)),
    [?assertEqual({error, backoff}, spacepush_cache:get(?URL)) || _ <- lists:seq(1, 10)],
    ?assertEqual({error, backoff}, spacepush_cache:refresh(?URL)),
    ?assertMatch({ok, _}, spacepush_cache:refresh(Other)),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)).

successful_attempts_keep_minimum_interval() ->
    application:set_env(spacepush, fetch_min_interval_ms, 60000),
    application:set_env(spacepush, cache_max_age_ms, 0),
    spacepush_test_util:fake_responses(#{?URL => {ok, ?SPACE}}),
    {ok, Entry} = spacepush_cache:refresh(?URL),
    ?assertEqual({ok, Entry}, spacepush_cache:get(?URL)),
    ?assertEqual({ok, Entry}, spacepush_cache:refresh(?URL)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)),
    sys:replace_state(spacepush_cache, fun(#{attempts := Attempts} = State) ->
        Attempt = maps:get(?URL, Attempts),
        State#{attempts := Attempts#{?URL := Attempt#{next := erlang:monotonic_time(millisecond) - 1}}}
    end),
    ?assertMatch({ok, _}, spacepush_cache:refresh(?URL)),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)).

failure_backoff_grows_caps_and_resets() ->
    application:set_env(spacepush, fetch_min_interval_ms, 100),
    application:set_env(spacepush, fetch_backoff_max_ms, 400),
    spacepush_test_util:fake_responses(#{?URL => {error, timeout}}),
    lists:foreach(fun(ExpectedDelay) ->
        ?assertEqual({error, timeout}, spacepush_cache:refresh(?URL)),
        #{attempts := #{?URL := #{delay := Delay}}} = sys:get_state(spacepush_cache),
        ?assertEqual(ExpectedDelay, Delay),
        Calls = spacepush_test_util:fake_calls(x),
        ?assertEqual({error, backoff}, spacepush_cache:refresh(?URL)),
        ?assertEqual(Calls, spacepush_test_util:fake_calls(x)),
        wait_until_due()
    end, [100, 200, 400, 400]),
    spacepush_test_util:fake_responses(#{?URL => {ok, ?SPACE}}),
    ?assertMatch({ok, _}, spacepush_cache:refresh(?URL)),
    #{attempts := #{?URL := #{delay := 0}}} = sys:get_state(spacepush_cache),
    wait_until_due(),
    spacepush_test_util:fake_responses(#{?URL => {error, timeout}}),
    ?assertEqual({error, timeout}, spacepush_cache:refresh(?URL)),
    #{attempts := #{?URL := #{delay := 100}}} = sys:get_state(spacepush_cache).

wait_until_due() ->
    #{attempts := #{?URL := #{next := Next}}} = sys:get_state(spacepush_cache),
    timer:sleep(max(0, Next - erlang:monotonic_time(millisecond)) + 1).

shared_failure_only_falls_back_for_readers() ->
    application:set_env(spacepush, fetch_min_interval_ms, 60000),
    application:set_env(spacepush, cache_max_age_ms, 0),
    spacepush_cache:store(?URL, ?SPACE, []),
    Stale = spacepush_cache:lookup(?URL),
    {_Caller, Transport} = start_waiting_fetch({error, timeout}),
    Self = self(),
    spawn(fun() -> Self ! {refresh_result, spacepush_cache:refresh(?URL)} end),
    wait_for_callers(2, erlang:monotonic_time(millisecond) + 1000),
    Transport ! continue,
    ?assertEqual({ok, Stale}, fetch_result()),
    ?assertEqual({error, timeout}, receive {refresh_result, Result} -> Result after 1000 -> timeout end),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)),
    ?assertEqual(Stale, spacepush_cache:lookup(?URL)).

unusable_mainframe_keeps_last_good_result() ->
    {ok, Configured} = application:get_env(spacepush, mainframe_url),
    Url = list_to_binary(Configured),
    Body = spacepush_test_util:fixture("mainframe-openstate.json"),
    spacepush_test_util:fake_responses(#{Url => {ok, Body}}),
    {ok, Good} = spacepush_cache:refresh(Url),
    application:set_env(spacepush, cache_max_age_ms, 0),
    spacepush_test_util:fake_responses(#{Url => {ok, <<"{\"error\":\"maintenance\"}">>}}),
    ?assertEqual({error, invalid_response}, spacepush_cache:refresh(Url)),
    ?assertEqual(Good, spacepush_cache:lookup(Url)),
    ?assertEqual({ok, Good}, spacepush_cache:get(Url)),
    ?assertEqual(Good, spacepush_cache:lookup(Url)).
