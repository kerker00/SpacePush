-module(spacepush_commit_tests).
-include_lib("eunit/include/eunit.hrl").

%% These helpers run only in isolated local peer VMs.
-export([crash_commit/2, recover_in_fresh_node/1]).

-define(TOPIC, {<<"https://commit-test.example/">>, <<"space">>}).

token(N) -> binary:copy(integer_to_binary(N), 64).
tracker(State) -> #{?TOPIC => #{confirmed => State, candidate => none}}.
change(From, To) -> {?TOPIC, <<"Commit test">>, From, To}.
now_ms() -> erlang:system_time(millisecond).

configure(Dir) ->
    _ = application:load(spacepush),
    [application:set_env(spacepush, Key, filename:join(Dir, Name)) || {Key, Name} <- [
        {registry_file, "registry.dets"}, {outbox_file, "outbox.dets"}, {tracker_file, "tracker.bin"}
    ]],
    application:set_env(spacepush, max_registrations, 3),
    application:set_env(spacepush, registration_ttl_days, 60),
    application:set_env(spacepush, notification_cooldown_ms, 0),
    application:set_env(spacepush, delivery_ttl_ms, 60000).

start(Module) ->
    {ok, Pid} = Module:start_link(),
    unlink(Pid).

setup() ->
    Dir = filename:absname(spacepush_test_util:tmp_dir("commit")),
    configure(Dir),
    start(spacepush_registry),
    start(spacepush_outbox),
    [ok = spacepush_registry:register(token(N), sandbox, [?TOPIC]) || N <- [1, 2]],
    {ok, File} = application:get_env(spacepush, tracker_file),
    ok = spacepush_store:save(File, spacepush_state:snapshot(tracker(closed))),
    File.

cleanup(_) ->
    application:unset_env(spacepush, tracker_commit_hook),
    [case whereis(M) of undefined -> ok; _ -> gen_server:stop(M) end || M <- [spacepush_outbox, spacepush_registry]].

commit_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun sender_completion_before_poller_crash/0,
        fun corrupt_intent_is_not_ignored/0,
        fun invalid_intent_records_are_rejected/0,
        fun invalid_outbox_marker_prevents_start/0,
        fun unfinished_commit_is_not_overwritten/0,
        fun candidate_only_snapshot_has_no_intent/0
    ]}.

complete_all() ->
    [ok = spacepush_outbox:complete(Key, Id) || #{key := Key, id := Id} <- spacepush_outbox:due(now_ms(), [], 10)],
    ?assertEqual(0, spacepush_outbox:pending()),
    ok.

sender_completion_before_poller_crash() ->
    {ok, File} = application:get_env(spacepush, tracker_file),
    Parent = self(),
    application:set_env(spacepush, tracker_commit_hook, fun
        (outbox_saved) -> Parent ! {commit_paused, self()}, receive continue_commit -> ok end;
        (_) -> ok
    end),
    {Writer, Monitor} = spawn_monitor(fun() -> spacepush_commit:commit(File, tracker(open), [change(closed, open)]) end),
    try
        receive {commit_paused, Writer} -> ok after 2000 -> error(commit_not_paused) end,
        {Id, _Snapshot, Changes} = spacepush_commit:pending(File),
        Outbox = whereis(spacepush_outbox),
        %% A running sender may complete every opening before the poller dies.
        complete_all(),
        exit(Writer, kill),
        receive {'DOWN', Monitor, process, Writer, killed} -> ok after 2000 -> error(writer_not_dead) end,
        application:unset_env(spacepush, tracker_commit_hook),
        {ok, #{tracker := Restored}} = spacepush_poller:init([]),
        ?assertEqual(tracker(open), Restored),
        ?assertEqual(Outbox, whereis(spacepush_outbox)),
        ?assertEqual(0, spacepush_outbox:pending()),
        ok = spacepush_outbox:apply_commit(Id, Changes),
        ?assertEqual(0, spacepush_outbox:pending()),
        %% The next confirmed closing must still generate the counter-message.
        ?assertMatch({_, [{{_, _}, _, open, closed}]}, confirm_closing(Restored)),
        receive poll -> ok after 0 -> ok end
    after
        application:unset_env(spacepush, tracker_commit_hook),
        Writer ! continue_commit,
        exit(Writer, kill)
    end.

corrupt_intent_is_not_ignored() ->
    {ok, File} = application:get_env(spacepush, tracker_file),
    ok = file:write_file(File ++ ".intent", <<"broken">>),
    ?assertError(invalid_tracker_intent, spacepush_commit:pending(File)),
    ?assertError(invalid_tracker_intent, spacepush_commit:recover(File)),
    gen_server:stop(spacepush_outbox),
    %% Start in a monitored process to check the gate without linking the test.
    {Pid, Ref} = spawn_monitor(fun() -> spacepush_outbox:init([]) end),
    receive {'DOWN', Ref, process, Pid, Reason} ->
        ?assertMatch({invalid_tracker_intent, _}, Reason)
    after 2000 -> error(outbox_start_not_rejected) end.

invalid_intent_records_are_rejected() ->
    {ok, File} = application:get_env(spacepush, tracker_file),
    Id = binary:copy(<<0>>, 16),
    Snapshot = spacepush_state:snapshot(tracker(open)),
    [begin
        ok = file:write_file(File ++ ".intent", term_to_binary(Term)),
        ?assertError(invalid_tracker_intent, spacepush_commit:pending(File))
    end || Term <- [
        {tracker_commit, 99, Id, Snapshot, [change(closed, open)]},
        {tracker_commit, 1, <<0>>, Snapshot, [change(closed, open)]},
        {tracker_commit, 1, Id, {tracker, 1, #{?TOPIC => invalid}}, [change(closed, open)]},
        {tracker_commit, 1, Id, Snapshot, [change(open, closed)]},
        {tracker_commit, 1, Id, Snapshot, [garbage]},
        {tracker_commit, 1, Id, Snapshot, not_a_list}
    ]].

unfinished_commit_is_not_overwritten() ->
    {ok, File} = application:get_env(spacepush, tracker_file),
    Id = binary:copy(<<0>>, 16),
    Intent = {tracker_commit, 1, Id, spacepush_state:snapshot(tracker(open)), [change(closed, open)]},
    ok = spacepush_store:save(File ++ ".intent", Intent),
    ?assertError(unfinished_tracker_commit, spacepush_commit:commit(File, tracker(closed), [])),
    ?assertEqual(tracker(open), spacepush_commit:recover(File)),
    ?assertEqual(2, spacepush_outbox:pending()).

invalid_outbox_marker_prevents_start() ->
    gen_server:stop(spacepush_outbox),
    {ok, File} = application:get_env(spacepush, outbox_file),
    {ok, marker_fixture} = dets:open_file(marker_fixture, [{file, File}, {type, set}]),
    ok = dets:insert(marker_fixture, {'$spacepush_commit', {outbox_commit, 99, binary:copy(<<0>>, 16)}}),
    ok = dets:close(marker_fixture),
    {Pid, Ref} = spawn_monitor(fun() -> spacepush_outbox:init([]) end),
    receive {'DOWN', Ref, process, Pid, Reason} ->
        ?assertMatch({invalid_outbox_commit, _}, Reason)
    after 2000 -> error(outbox_start_not_rejected) end.

candidate_only_snapshot_has_no_intent() ->
    {ok, File} = application:get_env(spacepush, tracker_file),
    {Candidate, []} = spacepush_state:track(tracker(closed), [{?TOPIC, <<"Commit test">>, open}], 100, 5),
    ok = spacepush_commit:commit(File, Candidate, []),
    ?assertEqual(none, spacepush_commit:pending(File)),
    ?assertEqual(Candidate, spacepush_commit:recover(File)),
    ?assertEqual(0, spacepush_outbox:pending()).

confirm_closing(Open) ->
    Observations = [{?TOPIC, <<"Commit test">>, closed}],
    {Candidate, []} = spacepush_state:track(Open, Observations, 100, 5),
    spacepush_state:track(Candidate, Observations, 105, 5).

abrupt_commit_recovery_test_() ->
    [{atom_to_list(Stage), {timeout, 30, fun() -> abrupt_commit_recovery(Stage) end}} || Stage <- [
        intent_saved, delivery_written, deliveries_saved, outbox_saved,
        sender_completed, tracker_saved, intent_cleared
    ]].

peer_start() ->
    Paths = lists:append([["-pa", Path] || Path <- code:get_path(), string:find(Path, "_build") =/= nomatch]),
    case peer:start(#{connection => standard_io, args => Paths}) of
        {ok, Pid} -> Pid;
        {ok, Pid, _Node} -> Pid
    end.

abrupt_commit_recovery(Stage) ->
    Dir = filename:absname(spacepush_test_util:tmp_dir("commit-halt-" ++ atom_to_list(Stage))),
    Writer = peer_start(),
    Monitor = monitor(process, Writer),
    try
        peer:cast(Writer, ?MODULE, crash_commit, [Dir, Stage]),
        receive {'DOWN', Monitor, process, Writer, _} -> ok after 10000 -> error(commit_vm_did_not_halt) end
    after try peer:stop(Writer) catch exit:_ -> ok end end,
    Recovery = peer_start(),
    try
        Result = peer:call(Recovery, ?MODULE, recover_in_fresh_node, [Dir]),
        ExpectedPending = case Stage of sender_completed -> 0; _ -> 2 end,
        ?assertMatch(#{confirmed := open, replay_unchanged := true, intent_cleared := true,
            closing_confirmed := closed, closing_pending := 2}, Result),
        ?assertEqual(ExpectedPending, maps:get(pending_before_recover, Result)),
        ?assertEqual(ExpectedPending, maps:get(pending_after_recover, Result))
    after peer:stop(Recovery) end.

crash_commit(Dir, CrashStage) ->
    configure(Dir),
    start(spacepush_registry),
    start(spacepush_outbox),
    [ok = spacepush_registry:register(token(N), sandbox, [?TOPIC]) || N <- [1, 2]],
    {ok, File} = application:get_env(spacepush, tracker_file),
    ok = spacepush_store:save(File, spacepush_state:snapshot(tracker(closed))),
    application:set_env(spacepush, tracker_commit_hook, fun(Stage) ->
        case {CrashStage, Stage} of
            {sender_completed, outbox_saved} -> complete_all(), erlang:halt(0, [{flush, false}]);
            {Stage, Stage} ->
                %% Force the partially-written row onto disk before halting.
                Stage =:= delivery_written andalso dets:sync(spacepush_outbox),
                erlang:halt(0, [{flush, false}]);
            _ -> ok
        end
    end),
    spacepush_commit:commit(File, tracker(open), [change(closed, open)]),
    error(crash_stage_not_reached).

recover_in_fresh_node(Dir) ->
    configure(Dir),
    start(spacepush_registry),
    %% Startup itself must finish partial rows before APNs could start.
    start(spacepush_outbox),
    Before = spacepush_outbox:pending(),
    DueBefore = spacepush_outbox:due(now_ms(), [], 10),
    {ok, File} = application:get_env(spacepush, tracker_file),
    Open = spacepush_commit:recover(File),
    After = spacepush_outbox:pending(),
    Replay = spacepush_commit:recover(File),
    DueAfter = spacepush_outbox:due(now_ms(), [], 10),
    {Closed, Changes} = confirm_closing(Open),
    ?assertEqual([change(open, closed)], Changes),
    ok = spacepush_commit:commit(File, Closed, Changes),
    Closing = spacepush_outbox:due(now_ms(), [], 10),
    ?assertEqual([<<"closed">>, <<"closed">>], [maps:get(<<"state">>, json:decode(Payload)) || #{payload := Payload} <- Closing]),
    Result = #{
        confirmed => maps:get(confirmed, maps:get(?TOPIC, Open)),
        pending_before_recover => Before, pending_after_recover => After,
        replay_unchanged => Replay =:= Open andalso DueBefore =:= DueAfter,
        intent_cleared => spacepush_commit:pending(File) =:= none,
        closing_confirmed => maps:get(confirmed, maps:get(?TOPIC, spacepush_commit:recover(File))),
        closing_pending => length(Closing)
    },
    cleanup(File),
    Result.
