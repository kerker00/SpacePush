-module(spacepush_stats_tests).
-include_lib("eunit/include/eunit.hrl").

-define(APP, <<"SpaceState/2.0.0 (iOS 26.0)">>).

first_week_label_test_() ->
    [
        ?_assertEqual(<<"2026-W41">>, spacepush_stats:week_label({2026, 10, 11})),
        %% The ISO week of 2027-01-03 belongs to 2026.
        ?_assertEqual(<<"2026-W53">>, spacepush_stats:week_label({2027, 1, 3}))
    ].

server_test_() ->
    {foreach, fun setup/0, fun(_) -> stop() end, [
        fun counts_requests_and_installs/0,
        fun ignores_installs_of_other_clients/0,
        fun counts_events/0,
        fun survives_restart/0,
        fun reports_past_periods/0,
        fun migrates_format_1/0,
        fun reports_devices/0
    ]}.

setup() ->
    spacepush_test_util:setup_env("stats"),
    lists:foreach(fun start/1, [spacepush_registry, spacepush_directory, spacepush_stats]).

start(Module) ->
    {ok, Pid} = Module:start_link(),
    unlink(Pid).

stop() ->
    lists:foreach(fun gen_server:stop/1, [spacepush_stats, spacepush_directory, spacepush_registry]).

req(Headers) ->
    #{headers => Headers, peer => {{127, 0, 0, 1}, 4711}}.

app(First) ->
    req(#{<<"user-agent">> => ?APP, <<"x-spacestate-first">> => First}).

app() ->
    req(#{<<"user-agent">> => ?APP}).

today(Report) ->
    hd(maps:get(<<"days">>, Report)).

counts_requests_and_installs() ->
    spacepush_stats:request(<<"directory">>, app(<<"day, week, month">>)),
    spacepush_stats:request(<<"directory">>, app()),
    spacepush_stats:request(<<"space">>, app(<<"day">>)),
    Report = spacepush_stats:report(7),
    ?assertMatch(#{<<"requests.directory">> := 2, <<"requests.space">> := 1}, today(Report)),
    ?assertEqual(7, length(maps:get(<<"days">>, Report))),
    Installs = maps:get(<<"installs">>, Report),
    ?assertMatch(#{<<"today">> := 2, <<"this_week">> := 1, <<"this_month">> := 1}, Installs),
    ?assertMatch(
        #{<<"versions">> := #{<<"2.0.0">> := 1}, <<"platforms">> := #{<<"iOS">> := 1}, <<"os_versions">> := #{<<"iOS 26.0">> := 1}},
        maps:get(<<"this_week_by">>, Installs)
    ).

ignores_installs_of_other_clients() ->
    spacepush_stats:request(<<"directory">>, req(#{<<"x-spacestate-first">> => <<"day">>})),
    spacepush_stats:request(<<"directory">>, req(#{<<"user-agent">> => <<"SpaceStateWidget/2.0.0 (iOS 26.0)">>, <<"x-spacestate-first">> => <<"day">>})),
    %% Earlier app builds sent an install ID instead; it is ignored.
    spacepush_stats:request(<<"directory">>, req(#{<<"user-agent">> => ?APP, <<"x-spacestate-install">> => <<"0f8fad5b-d9cb-469f-a165-70867728950e">>})),
    Report = spacepush_stats:report(1),
    ?assertMatch(#{<<"requests.directory">> := 3, <<"requests.other">> := 1, <<"requests.widget">> := 1}, today(Report)),
    ?assertNot(maps:is_key(<<"installs.day">>, today(Report))),
    ?assertMatch(#{<<"today">> := 0}, maps:get(<<"installs">>, Report)).

counts_events() ->
    spacepush_stats:count(<<"push.delivered">>),
    spacepush_stats:count(<<"push.delivered">>, 2),
    spacepush_stats:rate_limited(read),
    spacepush_stats:count(<<"poll.rounds">>, 2),
    spacepush_stats:duration(<<"poll">>, 30),
    spacepush_stats:duration(<<"poll">>, 10),
    spacepush_stats:success(<<"directory">>),
    Report = spacepush_stats:report(1),
    ?assertMatch(
        #{<<"push.delivered">> := 3, <<"rate_limited.read">> := 1, <<"poll.ms_sum">> := 40, <<"poll.ms_max">> := 30, <<"poll.ms_avg">> := 20},
        today(Report)
    ),
    ?assertMatch(#{<<"directory">> := _}, maps:get(<<"last_success">>, Report)),
    %% The report must be encodable as JSON.
    ?assert(is_binary(iolist_to_binary(json:encode(Report)))).

survives_restart() ->
    spacepush_stats:request(<<"directory">>, app(<<"day, week">>)),
    spacepush_stats:count(<<"push.delivered">>),
    gen_server:stop(spacepush_stats),
    start(spacepush_stats),
    spacepush_stats:request(<<"directory">>, app(<<"day">>)),
    Report = spacepush_stats:report(1),
    ?assertMatch(#{<<"requests.directory">> := 2, <<"push.delivered">> := 1}, today(Report)),
    Installs = maps:get(<<"installs">>, Report),
    ?assertMatch(#{<<"today">> := 2, <<"this_week">> := 1}, Installs),
    ?assertMatch(#{<<"versions">> := #{<<"2.0.0">> := 1}}, maps:get(<<"this_week_by">>, Installs)).

%% Writes a file as if the service had run on earlier days.
reports_past_periods() ->
    gen_server:stop(spacepush_stats),
    Day = fun(N) -> calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(date()) - N) end,
    Days = #{Day(3) => #{<<"installs.day">> => 2}, Day(2) => #{<<"installs.day">> => 1}, Day(1) => #{<<"requests.space">> => 4}},
    {ok, File} = application:get_env(spacepush, stats_file),
    spacepush_store:save(File, {stats, 2, Days, #{}, #{}}),
    start(spacepush_stats),
    History = maps:get(<<"days">>, maps:get(<<"installs">>, spacepush_stats:report(1))),
    ?assertEqual(
        [{label(Day(1)), 0}, {label(Day(2)), 1}, {label(Day(3)), 2}],
        [{Period, N} || #{<<"period">> := Period, <<"installs">> := N} <- History]
    ).

%% Format 1 held hashed install IDs; only its counters survive.
migrates_format_1() ->
    gen_server:stop(spacepush_stats),
    Yesterday = calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(date()) - 1),
    Installs = #{Yesterday => #{<<"hash">> => {<<"2.0.0">>, <<"iOS">>, <<"26.0">>}}},
    {ok, File} = application:get_env(spacepush, stats_file),
    spacepush_store:save(File, {stats, 1, <<"salt">>, #{Yesterday => #{<<"push.delivered">> => 3}}, Installs, #{}, #{}, Yesterday}),
    start(spacepush_stats),
    Report = spacepush_stats:report(2),
    ?assertMatch([_Today, #{<<"push.delivered">> := 3}], maps:get(<<"days">>, Report)),
    gen_server:stop(spacepush_stats),
    ?assertMatch({stats, 2, _Days, #{}, #{}}, spacepush_store:load(File, none)),
    start(spacepush_stats).

label(Date) ->
    list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", tuple_to_list(Date))).

reports_devices() ->
    Topic = {<<"https://a.example/">>, <<"space">>},
    ok = spacepush_registry:register(binary:copy(<<"1">>, 64), production, [Topic], <<"ios">>),
    ok = spacepush_registry:register(binary:copy(<<"2">>, 64), sandbox, [Topic]),
    Devices = maps:get(<<"devices">>, spacepush_stats:report(1)),
    ?assertMatch(
        #{
            <<"total">> := 2,
            <<"by_environment">> := #{production := 1, sandbox := 1},
            <<"by_platform">> := #{<<"ios">> := 1, <<"unknown">> := 1},
            <<"subscriptions">> := [#{<<"endpoint">> := <<"https://a.example/">>, <<"devices">> := 2}]
        },
        Devices
    ),
    ?assertMatch(#{<<"registrations.new">> := 2}, today(spacepush_stats:report(1))).

local_test_() ->
    Req = fun(Ip, Headers) -> #{headers => Headers, peer => {Ip, 4711}} end,
    [
        ?_assert(spacepush_http_stats:local(Req({127, 0, 0, 1}, #{}))),
        ?_assert(spacepush_http_stats:local(Req({0, 0, 0, 0, 0, 0, 0, 1}, #{}))),
        ?_assertNot(spacepush_http_stats:local(Req({127, 0, 0, 1}, #{<<"x-forwarded-for">> => <<"203.0.113.7">>}))),
        ?_assertNot(spacepush_http_stats:local(Req({10, 0, 0, 1}, #{})))
    ].
