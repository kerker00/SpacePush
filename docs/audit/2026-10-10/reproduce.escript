#!/usr/bin/env escript
%% Local audit evidence, not a safety/regression pass.
%% Run from SpacePush after `rebar3 eunit` has compiled the test profile.
%% No network connections, private keys, production config or production data.
%% This script confirms defects at the audited revision. Fixed code may fail
%% these assertions; turn the cases into inverse regression tests when fixing.
-mode(compile).
-export([fake_get/2]).

main([]) ->
    true = filelib:is_file("src/spacepush.app.src"),
    Paths = filelib:wildcard("_build/test/lib/*/ebin"),
    ok = code:add_paths(Paths),
    {module, spacepush_state} = code:ensure_loaded(spacepush_state),
    logger:set_primary_config(level, emergency),
    %% Export private functions only in this disposable VM, without changing
    %% source files. response/4 is exercised with Gun's real await/3.
    {ok, spacepush_fetch, FetchBeam} = compile:file("src/spacepush_fetch.erl", [binary, export_all]),
    {module, spacepush_fetch} = code:load_binary(spacepush_fetch, "audit-memory-only", FetchBeam),
    Cases = [
        {"token_newline", fun token_newline/0},
        {"statistics_before_rate_limit", fun stats_growth/0},
        {"informational_response", fun informational_response/0},
        {"cache_worker_crash", fun cache_worker_crash/0},
        {"on_demand_refetch_storm", fun refetch_storm/0},
        {"poller_overwrites_newer_cache", fun cache_ordering/0},
        {"registry_capacity", fun registry_capacity/0},
        {"directory_freshness", fun directory_freshness/0},
        {"apns_environment_starvation", fun starvation/0},
        {"expired_replacement", fun expired_replacement/0},
        {"tracker_crash_boundary", fun tracker_boundary/0},
        {"malformed_mainframe", fun malformed_mainframe/0}
    ],
    try
        lists:foreach(fun({Name, Test}) ->
            reset(Name),
            Result = Test(),
            io:format("CONFIRMED ~s: ~p~n", [Name, Result]),
            cleanup()
        end, Cases),
        io:format("Audit cases confirmed: ~B. No external requests made.~n", [length(Cases)])
    after
        cleanup()
    end;
main(_) ->
    error(run_from_repository_root_without_arguments).

reset(Name) ->
    cleanup(),
    _ = application:unload(spacepush),
    ok = application:load(spacepush),
    Dir = filename:join(["_build", "test", "tmp", "audit-2026-10-10", Name]),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "placeholder")),
    [application:set_env(spacepush, Key, filename:join(Dir, File)) ||
        {Key, File} <- [{registry_file, "registry.dets"}, {outbox_file, "outbox.dets"},
                       {stats_file, "stats.bin"}, {tracker_file, "tracker.bin"},
                       {directory_file, "directory.bin"}]],
    application:set_env(spacepush, http_get, {?MODULE, fake_get}),
    persistent_term:put({?MODULE, fake_mode}, crash),
    ok.

cleanup() ->
    [case whereis(M) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 1000)
     end || M <- [spacepush_apns, spacepush_cache, spacepush_stats,
                  spacepush_outbox, spacepush_registry, spacepush_ratelimit]],
    case ets:info(spacepush_directory, owner) of
        undefined -> ok;
        Owner when Owner =:= self() -> ets:delete(spacepush_directory)
    end,
    ok.

start(Modules) ->
    [begin {ok, Pid} = M:start_link(), unlink(Pid) end || M <- Modules],
    ok.

token(N) ->
    iolist_to_binary(io_lib:format("~64.16.0B", [N])).

topic() -> {<<"https://audit.example/">>, <<"space">>}.

sender_state() ->
    #{keys => #{production => unused}, paused => #{},
      connections => #{production => #{pid => self(), up => true}},
      tokens => #{production => {<<"synthetic-audit-provider-token">>, erlang:system_time(second)}},
      renew => #{}, topic => <<"audit.example">>, request_timeout_ms => 15000,
      in_flight => #{}, max_in_flight => 1}.

token_newline() ->
    Hex = token(1),
    Bad = <<Hex/binary, 10>>,
    true = spacepush_http_devices:valid_token(Bad),
    Dispatch = cowboy_router:compile([{'_', [{"/v1/devices/:token", spacepush_http_devices, []}]}]),
    {ok, Routed, _} = cowboy_router:execute(
        #{host => <<"localhost">>, path => <<"/v1/devices/", Hex/binary, "%0A">>},
        #{dispatch => Dispatch}),
    Bad = cowboy_req:binding(token, Routed),
    start([spacepush_registry, spacepush_outbox]),
    ok = spacepush_registry:register(Bad, production, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Audit">>, closed, open}]),
    Failure = try spacepush_apns:handle_cast(wake, sender_state()) of
        _ -> unexpected_success
    catch error:{invalid_request_line, path, _} -> invalid_request_line end,
    invalid_request_line = Failure,
    1 = spacepush_outbox:pending(),
    #{decoded_lf_accepted => true, dispatch_raises => Failure, persisted_delivery_remains => true}.

stats_growth() ->
    application:set_env(spacepush, read_rate_limit_per_minute, 1),
    start([spacepush_stats, spacepush_ratelimit]),
    Admission = [begin
        Version = integer_to_binary(N),
        Req = #{headers => #{<<"user-agent">> => <<"SpaceState/", Version/binary, " (iOS 26.0)">>,
                             <<"x-spacestate-first">> => <<"week">>}},
        ok = spacepush_stats:request(<<"summary">>, Req),
        spacepush_ratelimit:allow(read, audit_client)
    end || N <- lists:seq(1, 3)],
    [true, false, false] = Admission,
    #{weeks := Weeks} = sys:get_state(spacepush_stats),
    [#{<<"versions">> := Versions}] = maps:values(Weeks),
    3 = map_size(Versions),
    #{admission => Admission, distinct_persisted_version_keys => map_size(Versions)}.

informational_response() ->
    Ref = make_ref(),
    self() ! {gun_inform, self(), Ref, 103, []},
    Deadline = erlang:monotonic_time(millisecond) + 1000,
    Result = try spacepush_fetch:response(self(), Ref, Deadline, 262144) of
        _ -> unexpected_success
    catch error:{case_clause, {inform, 103, []}} -> case_clause_inform_103 end,
    case_clause_inform_103 = Result,
    Result.

cache_worker_crash() ->
    application:set_env(spacepush, on_demand_fetch_limit, 1),
    start([spacepush_cache]),
    Url = <<"https://audit.example/">>,
    First = try gen_server:call(spacepush_cache, {fetch, Url}, 40) of
        Value -> Value
    catch exit:{timeout, _} -> timeout end,
    timeout = First,
    Second = try gen_server:call(spacepush_cache, {fetch, Url}, 40) of
        Value2 -> Value2
    catch exit:{timeout, _} -> timeout end,
    timeout = Second,
    #{waiting := Waiting} = sys:get_state(spacepush_cache),
    1 = map_size(Waiting),
    2 = length(maps:get(Url, Waiting)),
    {error, busy} = gen_server:call(spacepush_cache, {fetch, <<"https://other-audit.example/">>}),
    #{abandoned_slots => map_size(Waiting), abandoned_callers => 2, other_url => busy}.

fake_get(_Url, _MaxBytes) ->
    case persistent_term:get({?MODULE, fake_mode}, crash) of
        crash -> error({case_clause, {inform, 103, []}});
        {failing, Counter} ->
            counters:add(Counter, 1, 1),
            {error, unavailable};
        {ordering, Parent, Counter} ->
            case _Url of
                "https://audit-a.example/" ->
                    counters:add(Counter, 1, 1),
                    case counters:get(Counter, 1) of
                        1 ->
                            Parent ! first_a_observed,
                            {ok, <<"{\"space\":\"Audit A\",\"state\":{\"open\":true}}">>};
                        _ -> {ok, <<"{\"space\":\"Audit A\",\"state\":{\"open\":false}}">>}
                    end;
                "https://audit-b.example/" ->
                    timer:sleep(180),
                    {ok, <<"{\"space\":\"Audit B\",\"state\":{\"open\":false}}">>};
                _ -> {ok, <<"{\"space\":{\"state\":\"closed\"}}">>}
            end
    end.

refetch_storm() ->
    Counter = counters:new(1, []),
    persistent_term:put({?MODULE, fake_mode}, {failing, Counter}),
    start([spacepush_cache]),
    Url = <<"https://audit.example/">>,
    Body = <<"{\"space\":\"Audit\",\"state\":{\"open\":true}}">>,
    true = spacepush_cache:store(Url, Body, [{topic(), <<"Audit">>, open}]),
    Old = (spacepush_cache:lookup(Url))#{fetched_at := erlang:system_time(millisecond) - 65000},
    ets:insert(spacepush_cache, {Url, Old}),
    [{ok, Old} = spacepush_cache:get(Url) || _ <- lists:seq(1, 4)],
    4 = counters:get(Counter, 1),
    #{sequential_reads => 4, upstream_failures_retried_immediately => 4}.

cache_ordering() ->
    Counter = counters:new(1, []),
    persistent_term:put({?MODULE, fake_mode}, {ordering, self(), Counter}),
    start([spacepush_registry, spacepush_outbox, spacepush_cache]),
    A = <<"https://audit-a.example/">>, B = <<"https://audit-b.example/">>,
    ets:new(spacepush_directory, [named_table, public, set]),
    ets:insert(spacepush_directory, [{A, #{endpoint => A}}, {B, #{endpoint => B}}]),
    ok = spacepush_registry:register(token(1), production, [{A, <<"space">>}, {B, <<"space">>}]),
    {ok, File} = application:get_env(spacepush, tracker_file),
    Parent = self(),
    {Pid, Ref} = spawn_monitor(fun() ->
        {noreply, _} = spacepush_poller:handle_info(poll,
            #{file => File, tracker => #{}, backoff => #{}, round => 0}),
        Parent ! poller_finished
    end),
    receive first_a_observed -> ok after 2000 -> error(no_first_observation) end,
    {ok, #{observations := [{_, _, closed}], fetched_at := ClosedAt}} = spacepush_cache:get(A),
    receive poller_finished -> ok after 2000 -> error(poller_not_finished) end,
    receive {'DOWN', Ref, process, Pid, normal} -> ok after 2000 -> error(poller_exit) end,
    #{observations := [{_, _, open}], fetched_at := OpenAt} = spacepush_cache:lookup(A),
    true = OpenAt > ClosedAt,
    #{newer_closed_replaced_by_older_open => true, older_observation_stamped_later => true}.

registry_capacity() ->
    application:set_env(spacepush, max_registrations, 2),
    {ok, production, [], <<"unknown">>} = spacepush_http_devices:registration(
        <<"{\"environment\":\"production\",\"subscriptions\":[]}">>),
    start([spacepush_registry]),
    ok = spacepush_registry:register(token(1), production, []),
    ok = spacepush_registry:register(token(2), production, []),
    {error, full} = spacepush_registry:register(token(3), production, [topic()]),
    #{empty_registrations_fill_cap => true, legitimate_registration => full}.

directory_freshness() ->
    start([spacepush_cache]),
    ets:new(spacepush_directory, [named_table, public, set]),
    [Entry] = spacepush_state:parse_directory(
        <<"[{\"url\":\"https://audit.example/\",\"lastSeen\":990,\"data\":{\"space\":\"Audit\",\"state\":{\"open\":true}}}]">>,
        1000, 900),
    ets:insert(spacepush_directory, {<<"https://audit.example/">>, Entry}),
    [#{<<"data">> := #{<<"state">> := #{<<"open">> := true}}}] = spacepush_http_read:directory(1901000),
    #{age_seconds => 911, max_age_seconds => 900, still_open => true}.

starvation() ->
    start([spacepush_registry, spacepush_outbox]),
    A = token(1), B = token(2),
    ok = spacepush_registry:register(A, sandbox, [topic()]),
    ok = spacepush_registry:register(B, production, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Audit">>, closed, open}]),
    Rows = spacepush_outbox:due(erlang:system_time(millisecond), [], 10),
    [DA] = [D || #{token := T} = D <- Rows, T =:= A],
    [DB] = [D || #{token := T} = D <- Rows, T =:= B],
    Now = erlang:system_time(millisecond),
    ok = spacepush_outbox:retry(maps:get(key, DA), maps:get(id, DA), Now - 2),
    ok = spacepush_outbox:retry(maps:get(key, DB), maps:get(id, DB), Now - 1),
    State = #{keys => #{sandbox => unused}, paused => #{}, in_flight => #{}, max_in_flight => 1,
              connections => #{sandbox => #{pid => self(), up => false}}},
    {noreply, S1} = spacepush_apns:handle_cast(wake, State),
    {noreply, _} = spacepush_apns:handle_cast(wake, S1),
    2 = spacepush_outbox:pending(),
    ok = spacepush_outbox:complete(maps:get(key, DA), maps:get(id, DA)),
    {noreply, _} = spacepush_apns:handle_cast(wake, S1),
    0 = spacepush_outbox:pending(),
    #{rows_after_two_dispatches => 2, healthy_sink_only_processed_after_blocker_removed => true}.

expired_replacement() ->
    application:set_env(spacepush, delivery_ttl_ms, 50),
    application:set_env(spacepush, notification_cooldown_ms, 0),
    start([spacepush_registry, spacepush_outbox]),
    ok = spacepush_registry:register(token(1), production, [topic()]),
    ok = spacepush_outbox:enqueue([{topic(), <<"Audit">>, closed, open}]),
    timer:sleep(70),
    ok = spacepush_outbox:enqueue([{topic(), <<"Audit">>, open, closed}]),
    1 = spacepush_outbox:pending(),
    [] = spacepush_outbox:due(erlang:system_time(millisecond), [], 10),
    0 = spacepush_outbox:pending(),
    #{new_payload_immediately_expired => true}.

tracker_boundary() ->
    T = topic(),
    Old = #{T => #{confirmed => closed, candidate => {open, 0}}},
    {New, [_Opening]} = spacepush_state:track(Old, [{T, <<"Audit">>, open}], 120, 120),
    {_Restored, []} = spacepush_state:track(Old, [{T, <<"Audit">>, closed}], 180, 120),
    {ClosingCandidate, []} = spacepush_state:track(New, [{T, <<"Audit">>, closed}], 180, 120),
    {_Closed, [_Closing]} = spacepush_state:track(ClosingCandidate, [{T, <<"Audit">>, closed}], 300, 120),
    #{restored_old_tracker_emits_no_closing => true, saved_new_tracker_would_emit_closing => true}.

malformed_mainframe() ->
    {ok, Url} = application:get_env(spacepush, mainframe_url),
    {ok, []} = spacepush_cache:observe(unicode:characters_to_binary(Url), <<"{\"error\":\"maintenance\"}">>),
    #{unusable_response_accepted_as => empty_observations}.
