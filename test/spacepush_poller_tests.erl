-module(spacepush_poller_tests).
-include_lib("eunit/include/eunit.hrl").

-define(A, <<"https://a.example/">>).
-define(B, <<"https://b.example/">>).
-define(SPACE, <<"{\"space\":\"A\",\"state\":{\"open\":true}}">>).
-define(TOPIC, {?A, <<"space">>}).

shared_fetch_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun background_space_is_refreshed_without_subscribers/0,
        fun on_demand_read_does_not_promote_background_space/0,
        fun mainframe_room_subscription_uses_active_interval/0,
        fun reader_failure_defers_poller_without_stale_observation/0,
        fun poller_failure_defers_reads/0,
        fun read_and_poll_share_one_worker/0,
        fun unrelated_endpoint_keeps_polling/0,
        fun poll_consumes_reader_generations_once/0,
        fun later_reader_response_supersedes_slow_round_result/0,
        fun aged_response_is_not_a_fresh_poll_observation/0
    ]}.

setup() ->
    spacepush_test_util:setup_env("poller"),
    application:set_env(spacepush, fetch_min_interval_ms, 60000),
    application:set_env(spacepush, fetch_backoff_max_ms, 1800000),
    application:set_env(spacepush, fetch_timeout_ms, 10000),
    application:set_env(spacepush, cache_max_age_ms, 0),
    ets:new(spacepush_directory, [named_table, protected, set]),
    ets:insert(spacepush_directory, [{?A, #{endpoint => ?A}}, {?B, #{endpoint => ?B}},
        {spacepush_state:mainframe_endpoint(), #{endpoint => spacepush_state:mainframe_endpoint()}}]),
    [begin {ok, Pid} = Module:start_link(), unlink(Pid) end || Module <- [spacepush_cache, spacepush_registry, spacepush_outbox]],
    ok = spacepush_registry:register(binary:copy(<<"ab">>, 32), sandbox, [?TOPIC]),
    ok.

cleanup(_) ->
    [gen_server:stop(Module) || Module <- [spacepush_outbox, spacepush_registry, spacepush_cache]],
    ets:delete(spacepush_directory).

responses(AResult) ->
    {ok, Mainframe} = application:get_env(spacepush, mainframe_url),
    #{?A => AResult, list_to_binary(Mainframe) => {ok, spacepush_test_util:fixture("mainframe-openstate.json")}}.

poll() ->
    {ok, File} = application:get_env(spacepush, tracker_file),
    {ok, Mainframe} = application:get_env(spacepush, mainframe_url),
    Now = erlang:monotonic_time(millisecond),
    poll(#{file => File, tracker => #{}, schedule => #{
        ?B => #{next => Now + 600000, interval => 600000},
        list_to_binary(Mainframe) => #{next => Now, interval => 600000}}}).

poll(Previous) ->
    {noreply, State} = spacepush_poller:handle_info(poll, Previous),
    State.

reader_failure_defers_poller_without_stale_observation() ->
    spacepush_cache:store(?A, ?SPACE, []),
    Stale = spacepush_cache:lookup(?A),
    spacepush_test_util:fake_responses(responses({error, timeout})),
    ?assertEqual({ok, Stale}, spacepush_cache:get(?A)),
    #{tracker := Tracker} = poll(),
    ?assertNot(maps:is_key(?TOPIC, Tracker)),
    ?assertEqual(Stale, spacepush_cache:lookup(?A)),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)).

poller_failure_defers_reads() ->
    spacepush_test_util:fake_responses(responses({error, timeout})),
    poll(),
    [?assertEqual({error, backoff}, spacepush_cache:get(?A)) || _ <- lists:seq(1, 10)],
    ?assertEqual(2, spacepush_test_util:fake_calls(x)).

read_and_poll_share_one_worker() ->
    spacepush_test_util:fake_responses(responses({notify, self(), {wait, {ok, ?SPACE}}})),
    Self = self(),
    spawn(fun() -> Self ! {read_result, spacepush_cache:get(?A)} end),
    Worker = receive {fetch_worker, Pid} -> Pid after 1000 -> error(no_worker) end,
    spawn(fun() -> Self ! {poll_result, poll()} end),
    wait_for_shared_callers(erlang:monotonic_time(millisecond) + 1000),
    Worker ! continue,
    Entry = receive {read_result, {ok, E}} -> E after 1000 -> error(read_timeout) end,
    #{tracker := Tracker} = receive {poll_result, S} -> S after 1000 -> error(poll_timeout) end,
    ?assertMatch(#{?TOPIC := #{confirmed := open}}, Tracker),
    ?assertEqual(Entry, spacepush_cache:lookup(?A)),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)).

unrelated_endpoint_keeps_polling() ->
    spacepush_test_util:fake_responses((responses({error, timeout}))#{?B => {ok, ?SPACE}}),
    ?assertEqual({error, timeout}, spacepush_cache:get(?A)),
    ok = spacepush_registry:register(binary:copy(<<"cd">>, 32), sandbox, [{?B, <<"space">>}]),
    #{tracker := Tracker} = poll(),
    ?assertMatch(#{{?B, <<"space">>} := #{confirmed := open}}, Tracker),
    ?assertNot(maps:is_key(?TOPIC, Tracker)),
    ?assertEqual(3, spacepush_test_util:fake_calls(x)).

poll_consumes_reader_generations_once() ->
    spacepush_test_util:fake_responses(responses({ok, ?SPACE})),
    ?assertMatch({ok, _}, spacepush_cache:get(?A)),
    State1 = poll(),
    ?assertMatch(#{tracker := #{?TOPIC := #{confirmed := open}}}, State1),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)),
    %% A second round can reuse the response for reads, but cannot re-observe it.
    State1 = poll(State1),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)),
    Closed = <<"{\"space\":\"A\",\"state\":{\"open\":false}}">>,
    spacepush_test_util:fake_responses(responses({ok, Closed})),
    make_a_due(),
    ?assertMatch({ok, #{body := Closed}}, spacepush_cache:get(?A)),
    State2 = poll(State1),
    #{tracker := Tracker2} = State2,
    ?assertMatch(#{?TOPIC := #{confirmed := open, candidate := {closed, _}}}, Tracker2),
    %% Even after the debounce duration, this same generation is not new evidence.
    {ok, Debounce} = application:get_env(spacepush, debounce_s),
    Entry = maps:get(?TOPIC, Tracker2),
    Candidate = Entry#{candidate := {closed, erlang:system_time(second) - Debounce - 1}},
    State3 = State2#{tracker := Tracker2#{?TOPIC := Candidate}},
    State3 = poll(State3),
    ?assertEqual(0, spacepush_outbox:pending()),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)),
    %% A later actual fetch supplies a new generation and confirms the change.
    make_a_due(),
    ?assertMatch({ok, #{body := Closed}}, spacepush_cache:get(?A)),
    ?assertMatch(#{tracker := #{?TOPIC := #{confirmed := closed, candidate := none}}}, poll(State3)),
    ?assertEqual(1, spacepush_outbox:pending()),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)).

make_a_due() ->
    sys:replace_state(spacepush_cache, fun(#{attempts := Attempts} = State) ->
        Attempt = maps:get(?A, Attempts),
        State#{attempts := Attempts#{?A := Attempt#{next := erlang:monotonic_time(millisecond) - 1}}}
    end).

later_reader_response_supersedes_slow_round_result() ->
    ok = spacepush_registry:register(binary:copy(<<"cd">>, 32), sandbox, [{?B, <<"space">>}]),
    spacepush_test_util:fake_responses((responses({ok, ?SPACE}))#{?B => {notify, self(), {wait, {ok, ?SPACE}}}}),
    Self = self(),
    spawn(fun() -> Self ! {poll_result, poll()} end),
    Worker = receive {fetch_worker, Pid} -> Pid after 1000 -> error(no_worker) end,
    {ok, Old} = spacepush_cache:get(?A),
    Closed = <<"{\"space\":\"A\",\"state\":{\"open\":false}}">>,
    spacepush_test_util:fake_responses(responses({ok, Closed})),
    make_a_due(),
    {ok, New} = spacepush_cache:get(?A),
    ?assertNotEqual(maps:get(generation, Old), maps:get(generation, New)),
    Worker ! continue,
    State = receive {poll_result, S} -> S after 1000 -> error(poll_timeout) end,
    ?assertNot(maps:is_key(?TOPIC, maps:get(tracker, State))),
    ?assertEqual(New, spacepush_cache:lookup(?A)),
    ?assertMatch(#{tracker := #{?TOPIC := #{confirmed := closed}}}, poll(State)),
    ?assertEqual(New, spacepush_cache:lookup(?A)).

aged_response_is_not_a_fresh_poll_observation() ->
    application:set_env(spacepush, max_data_age_s, 900),
    spacepush_test_util:fake_responses(responses({ok, ?SPACE})),
    {ok, Entry} = spacepush_cache:get(?A),
    Aged = Entry#{fetched_at := erlang:system_time(millisecond) - 901000},
    ets:insert(spacepush_cache, {?A, Aged}),
    #{tracker := Tracker} = poll(),
    ?assertNot(maps:is_key(?TOPIC, Tracker)),
    ?assertEqual(Aged, spacepush_cache:lookup(?A)),
    ?assertEqual(2, spacepush_test_util:fake_calls(x)).

wait_for_shared_callers(Deadline) ->
    #{waiting := #{?A := #{callers := Callers}}} = sys:get_state(spacepush_cache),
    case map_size(Callers) of
        2 -> ok;
        _ ->
            ?assert(erlang:monotonic_time(millisecond) < Deadline),
            timer:sleep(1),
            wait_for_shared_callers(Deadline)
    end.

background_space_is_refreshed_without_subscribers() ->
    spacepush_test_util:fake_responses((responses({ok, ?SPACE}))#{?B => {ok, ?SPACE}}),
    State = poll(),
    #{schedule := Schedule} = State,
    Due = Schedule#{?B := #{next => erlang:monotonic_time(millisecond), interval => 600000}},
    Next = poll(State#{schedule := Due}),
    ?assertMatch(#{body := ?SPACE}, spacepush_cache:lookup(?B)),
    #{schedule := #{?A := #{interval := 120000}, ?B := #{interval := 600000}}} = Next,
    Calls = spacepush_test_util:fake_calls(x),
    ?assertEqual(Next, poll(Next)),
    ?assertEqual(Calls, spacepush_test_util:fake_calls(x)).

on_demand_read_does_not_promote_background_space() ->
    spacepush_test_util:fake_responses((responses({ok, ?SPACE}))#{?B => {ok, ?SPACE}}),
    #{schedule := Schedule} = State = poll(),
    #{next := Deadline} = maps:get(?B, Schedule),
    ?assertMatch({ok, #{body := ?SPACE}}, spacepush_cache:get(?B)),
    ?assert(lists:member(?B, spacepush_cache:watched(0))),
    Calls = spacepush_test_util:fake_calls(x),
    #{schedule := #{?B := #{next := Deadline, interval := 600000}}} = poll(State),
    ?assertEqual(Calls, spacepush_test_util:fake_calls(x)).

mainframe_room_subscription_uses_active_interval() ->
    MainframeEndpoint = spacepush_state:mainframe_endpoint(),
    {ok, Mainframe} = application:get_env(spacepush, mainframe_url),
    RoomUrl = list_to_binary(Mainframe),
    ok = spacepush_registry:register(binary:copy(<<"ab">>, 32), sandbox, [{MainframeEndpoint, <<"radstelle">>}]),
    spacepush_test_util:fake_responses((responses({ok, ?SPACE}))#{MainframeEndpoint => {ok, ?SPACE}}),
    #{schedule := Schedule} = State = poll(),
    ?assertMatch(#{interval := 120000}, maps:get(RoomUrl, Schedule)),
    ?assertMatch(#{body := _}, spacepush_cache:lookup(RoomUrl)),
    ok = spacepush_registry:unregister(binary:copy(<<"ab">>, 32)),
    #{schedule := Later} = poll(State),
    ?assertMatch(#{interval := 600000}, maps:get(RoomUrl, Later)).
