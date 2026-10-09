-module(spacepush_stats_tests).
-include_lib("eunit/include/eunit.hrl").

-define(INSTALL_A, <<"0f8fad5b-d9cb-469f-a165-70867728950e">>).
-define(INSTALL_B, <<"7c9e6679-7425-40de-944b-e07fc1f90ae7">>).
-define(APP, <<"SpaceState/2.0.0 (iOS 26.0)">>).

install_id_test_() ->
    [
        ?_assertEqual(?INSTALL_A, spacepush_stats:install_id(?INSTALL_A)),
        ?_assertEqual(?INSTALL_A, spacepush_stats:install_id(string:uppercase(?INSTALL_A))),
        ?_assertEqual(none, spacepush_stats:install_id(<<"not-a-uuid">>)),
        ?_assertEqual(none, spacepush_stats:install_id(<<"0f8fad5b-d9cb-469f-a165-70867728950z">>)),
        ?_assertEqual(none, spacepush_stats:install_id(undefined))
    ].

user_agent_test_() ->
    [
        ?_assertEqual({app, {<<"2.0.0">>, <<"iOS">>, <<"26.0">>}}, spacepush_stats:user_agent(?APP)),
        ?_assertEqual(
            {widget, {<<"2.0.0">>, <<"macOS">>, <<"26.1">>}},
            spacepush_stats:user_agent(<<"SpaceStateWidget/2.0.0 (macOS 26.1) CFNetwork/3826 Darwin/25.0.0">>)
        ),
        ?_assertEqual(other, spacepush_stats:user_agent(<<"SpaceState/1 CFNetwork/3826 Darwin/25.0.0">>)),
        ?_assertEqual(other, spacepush_stats:user_agent(<<"curl/8.7.1">>)),
        ?_assertEqual(other, spacepush_stats:user_agent(undefined))
    ].

periods_test_() ->
    Labels = fun(Date) -> [Label || {Label, _Dates} <- spacepush_stats:periods_closed_by(Date)] end,
    [
        ?_assertEqual([<<"2026-10-08">>], Labels({2026, 10, 8})),
        %% A Sunday closes its ISO week, Monday to Sunday.
        ?_assertEqual([<<"2026-10-11">>, <<"2026-W41">>], Labels({2026, 10, 11})),
        ?_assertEqual(
            {2026, 10, 5},
            hd(proplists:get_value(<<"2026-W41">>, spacepush_stats:periods_closed_by({2026, 10, 11})))
        ),
        ?_assertEqual([<<"2026-10-31">>, <<"2026-10">>], Labels({2026, 10, 31})),
        %% The ISO week of 2027-01-03 belongs to 2026.
        ?_assertEqual([<<"2027-01-03">>, <<"2026-W53">>], Labels({2027, 1, 3}))
    ].

server_test_() ->
    {foreach, fun setup/0, fun(_) -> stop() end, [
        fun counts_requests_and_installs/0,
        fun ignores_installs_of_other_clients/0,
        fun counts_events/0,
        fun survives_restart/0,
        fun closes_past_periods/0,
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

app(Install) ->
    req(#{<<"user-agent">> => ?APP, <<"x-spacestate-install">> => Install}).

today(Report) ->
    hd(maps:get(<<"days">>, Report)).

counts_requests_and_installs() ->
    spacepush_stats:request(<<"directory">>, app(?INSTALL_A)),
    spacepush_stats:request(<<"directory">>, app(?INSTALL_A)),
    spacepush_stats:request(<<"space">>, app(?INSTALL_B)),
    Report = spacepush_stats:report(7),
    ?assertMatch(#{<<"requests.directory">> := 2, <<"requests.space">> := 1}, today(Report)),
    ?assertEqual(7, length(maps:get(<<"days">>, Report))),
    Installs = maps:get(<<"installs">>, Report),
    ?assertMatch(#{<<"today">> := 2, <<"this_week">> := 2, <<"this_month">> := 2}, Installs),
    ?assertMatch(
        #{<<"installs">> := 2, <<"versions">> := #{<<"2.0.0">> := 2}, <<"os_versions">> := #{<<"iOS 26.0">> := 2}},
        maps:get(<<"last_7_days">>, Installs)
    ).

ignores_installs_of_other_clients() ->
    spacepush_stats:request(<<"directory">>, req(#{<<"x-spacestate-install">> => ?INSTALL_A})),
    spacepush_stats:request(<<"directory">>, req(#{<<"user-agent">> => <<"SpaceStateWidget/2.0.0 (iOS 26.0)">>})),
    spacepush_stats:request(<<"directory">>, req(#{<<"user-agent">> => ?APP})),
    Report = spacepush_stats:report(1),
    ?assertMatch(
        #{<<"requests.directory">> := 3, <<"requests.other">> := 1, <<"requests.widget">> := 1, <<"requests.without_install">> := 1},
        today(Report)
    ),
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
    spacepush_stats:request(<<"directory">>, app(?INSTALL_A)),
    spacepush_stats:count(<<"push.delivered">>),
    gen_server:stop(spacepush_stats),
    start(spacepush_stats),
    spacepush_stats:request(<<"directory">>, app(?INSTALL_A)),
    Report = spacepush_stats:report(1),
    ?assertMatch(#{<<"requests.directory">> := 2, <<"push.delivered">> := 1}, today(Report)),
    ?assertMatch(#{<<"today">> := 1}, maps:get(<<"installs">>, Report)).

%% Writes a file as if the service had run on earlier days, then lets it close them.
closes_past_periods() ->
    gen_server:stop(spacepush_stats),
    Today = date(),
    Day = fun(N) -> calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(Today) - N) end,
    Info = {<<"2.0.0">>, <<"iOS">>, <<"26.0">>},
    Installs = #{Day(3) => #{<<"a">> => Info, <<"b">> => Info}, Day(2) => #{<<"a">> => Info}},
    {ok, File} = application:get_env(spacepush, stats_file),
    spacepush_store:save(File, {stats, 1, <<"salt">>, #{}, Installs, #{}, #{}, Day(5)}),
    start(spacepush_stats),
    Days = maps:get(<<"days">>, maps:get(<<"installs">>, spacepush_stats:report(1))),
    Label = fun(Date) -> list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", tuple_to_list(Date))) end,
    ?assertEqual(
        [{Label(Day(1)), 0}, {Label(Day(2)), 1}, {Label(Day(3)), 2}, {Label(Day(4)), 0}],
        [{Period, N} || #{<<"period">> := Period, <<"installs">> := N} <- Days]
    ).

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
