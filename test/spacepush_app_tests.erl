-module(spacepush_app_tests).
-moduledoc "Starts the whole application and talks to its HTTP API.".
-include_lib("eunit/include/eunit.hrl").

-define(TOKEN, "abababababababababababababababababababababababababababababababab").
-define(NERD2NERD, <<"https://api.nerd2nerd.org/status.json">>).
-define(NERD2NERD_QS, "https%3A%2F%2Fapi.nerd2nerd.org%2Fstatus.json").
-define(TOPIC, {?NERD2NERD, <<"space">>}).
-define(SPACE, <<"{\"space\":\"Nerd2Nerd\",\"state\":{\"open\":true}}">>).

app_test_() ->
    {setup, fun start/0, fun stop/1, fun(Port) ->
        [
            ?_test(all_children_running()),
            ?_assertEqual({200, <<"ok">>}, request(get, Port, "/health")),
            ?_test(register_and_unregister(Port)),
            ?_test(unconfirmed_admission_over_http(Port)),
            ?_test(empty_subscriptions_unregister_without_directory(Port)),
            ?_test(rejects_percent_decoded_token_bytes(Port)),
            ?_assertEqual(400, status(request(put, Port, "/v1/devices/xyz", <<"{}">>))),
            ?_assertEqual(413, status(request(put, Port, "/v1/devices/" ?TOKEN, binary:copy(<<" ">>, 20000)))),
            ?_assertEqual(413, raw(Port, chunked_oversized())),
            ?_assertEqual(408, raw(Port, incomplete_body())),
            ?_assertEqual(405, status(request(get, Port, "/v1/devices/" ?TOKEN))),
            ?_assertEqual(400, status(request(put, Port, "/v1/devices/" ?TOKEN, registration(<<"https://evil.example/">>)))),
            ?_test(directory(Port)),
            ?_test(summary(Port)),
            ?_assertEqual({200, ?SPACE}, request(get, Port, "/v1/spaces?endpoint=" ?NERD2NERD_QS)),
            ?_assertEqual(404, status(request(get, Port, "/v1/spaces?endpoint=http%3A%2F%2F127.0.0.1%3A8080%2F"))),
            ?_assertEqual(400, status(request(get, Port, "/v1/spaces"))),
            ?_assertEqual({200, spacepush_test_util:fixture("mainframe-openstate.json")}, request(get, Port, "/v1/mainframe/rooms")),
            ?_assertEqual(405, status(request(post, Port, "/v1/directory", <<"{}">>))),
            ?_test(passes_documents_through_safely(Port)),
            ?_test(document_freshness_headers(Port)),
            ?_test(expired_documents_are_not_successful_reads(Port)),
            ?_test(future_documents_are_not_successful_reads(Port)),
            ?_test(recent_fallback_preserves_original_fetch_time(Port)),
            ?_test(test_push_endpoint(Port)),
            ?_test(statistics_require_configured_credentials(Port)),
            ?_test(statistics_reject_invalid_logins(Port)),
            ?_test(statistics_stay_hidden_through_proxies(Port)),
            ?_test(statistics_and_dashboard_work_with_login(Port)),
            ?_test(rate_limited_requests_do_not_tally_versions(Port))
        ]
    end}.

start() ->
    spacepush_test_util:setup_env("app"),
    application:set_env(spacepush, http_port, 0),
    application:set_env(spacepush, http_body_timeout_ms, 500),
    application:set_env(spacepush, max_data_age_s, 1000000000),
    {ok, Aggregator} = application:get_env(spacepush, aggregator_url),
    {ok, Mainframe} = application:get_env(spacepush, mainframe_url),
    spacepush_test_util:fake_responses(#{
        list_to_binary(Aggregator) => {ok, spacepush_test_util:fixture("aggregator.json")},
        list_to_binary(Mainframe) => {ok, spacepush_test_util:fixture("mainframe-openstate.json")},
        ?NERD2NERD => {ok, ?SPACE}
    }),
    {ok, _} = application:ensure_all_started(spacepush),
    %% Wait until the directory has its first list.
    sys:get_state(spacepush_directory),
    ranch:get_port(spacepush_http).

stop(_Port) ->
    ok = application:stop(spacepush).

all_children_running() ->
    Children = supervisor:which_children(spacepush_sup),
    ?assertEqual(
        [
            spacepush_ratelimit,
            spacepush_registry,
            spacepush_directory,
            spacepush_cache,
            spacepush_outbox,
            spacepush_apns,
            spacepush_poller,
            spacepush_stats,
            spacepush_stream
        ],
        lists:reverse([Id || {Id, Pid, worker, _} <- Children, is_pid(Pid)])
    ).

registration(Endpoint) ->
    iolist_to_binary(json:encode(#{
        <<"environment">> => <<"sandbox">>,
        <<"subscriptions">> => [#{<<"endpoint">> => Endpoint}]
    })).

register_and_unregister(Port) ->
    ?assertEqual(204, status(request(put, Port, "/v1/devices/" ?TOKEN, registration(?NERD2NERD)))),
    ?assertMatch([{_, sandbox, _}], spacepush_registry:subscribers(?TOPIC)),
    ?assertEqual(204, status(request(delete, Port, "/v1/devices/" ?TOKEN))),
    ?assertEqual([], spacepush_registry:subscribers(?TOPIC)).

unconfirmed_admission_over_http(Port) ->
    {ok, OldCap} = application:get_env(spacepush, max_unconfirmed_registrations),
    Second = binary:copy(<<"cd">>, 32),
    FirstPath = "/v1/devices/" ?TOKEN,
    SecondPath = "/v1/devices/" ++ binary_to_list(Second),
    application:set_env(spacepush, max_unconfirmed_registrations, 1),
    try
        ?assertEqual(204, status(request(put, Port, FirstPath, registration(?NERD2NERD)))),
        ?assertEqual(503, status(request(put, Port, SecondPath, registration(?NERD2NERD)))),
        %% Renewal does not need an additional slot.
        ?assertEqual(204, status(request(put, Port, FirstPath, registration(?NERD2NERD)))),
        Empty = iolist_to_binary(json:encode(#{<<"environment">> => <<"sandbox">>, <<"subscriptions">> => []})),
        ?assertEqual(204, status(request(put, Port, FirstPath, Empty))),
        ?assertEqual(204, status(request(put, Port, SecondPath, registration(?NERD2NERD)))),
        ?assertEqual(204, status(request(delete, Port, SecondPath)))
    after
        application:set_env(spacepush, max_unconfirmed_registrations, OldCap),
        spacepush_registry:unregister(list_to_binary(?TOKEN)),
        spacepush_registry:unregister(Second)
    end.

empty_subscriptions_unregister_without_directory(Port) ->
    Path = "/v1/devices/" ?TOKEN,
    Empty = iolist_to_binary(json:encode(#{<<"environment">> => <<"sandbox">>, <<"subscriptions">> => []})),
    ?assertEqual(204, status(request(put, Port, Path, registration(?NERD2NERD)))),
    Entries = ets:tab2list(spacepush_directory),
    sys:replace_state(spacepush_directory, fun(State) -> ets:delete_all_objects(spacepush_directory), State end),
    try
        ?assertEqual(503, status(request(put, Port, Path, registration(?NERD2NERD)))),
        ?assertEqual(204, status(request(put, Port, Path, Empty))),
        ?assertEqual(error, spacepush_registry:lookup(list_to_binary(?TOKEN))),
        ?assertEqual([], spacepush_registry:subscribers(?TOPIC)),
        ?assertEqual(204, status(request(put, Port, Path, Empty))),
        ?assertEqual(error, spacepush_registry:lookup(list_to_binary(?TOKEN)))
    after
        sys:replace_state(spacepush_directory, fun(State) -> ets:insert(spacepush_directory, Entries), State end)
    end.

rejects_percent_decoded_token_bytes(Port) ->
    Path = "/v1/devices/" ?TOKEN,
    ?assertEqual(204, status(request(put, Port, Path, registration(?NERD2NERD)))),
    [
        begin
            ?assertEqual(400, status(request(put, Port, Path ++ Suffix, registration(?NERD2NERD)))),
            ?assertEqual(400, status(request(delete, Port, Path ++ Suffix)))
        end
     || Suffix <- ["%0A", "%0D", "%00", "%20", "%09"]
    ],
    %% An invalid DELETE must not remove the legitimate token sharing its prefix.
    ?assertMatch({ok, sandbox, [?TOPIC], _}, spacepush_registry:lookup(list_to_binary(?TOKEN))),
    ?assertEqual(204, status(request(delete, Port, Path))).

url(Port, Path) ->
    "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path.

request(Method, Port, Path) ->
    reply(httpc:request(Method, {url(Port, Path), []}, [], [{body_format, binary}])).

headers(Port, Path) ->
    {ok, {{_Version, _Status, _Phrase}, Headers, _Body}} = httpc:request(get, {url(Port, Path), []}, [], []),
    Headers.

request(Method, Port, Path, Body) ->
    reply(httpc:request(Method, {url(Port, Path), [], "application/json", Body}, [], [{body_format, binary}])).

reply({ok, {{_Version, Status, _Phrase}, _Headers, Body}}) -> {Status, Body}.

status({Status, _Body}) -> Status.

%% Requests httpc cannot send: a chunked body without a declared length, and
%% a body that stops before its declared length.
chunked_oversized() ->
    Chunk = binary:copy(<<" ">>, 10000),
    Size = integer_to_binary(byte_size(Chunk), 16),
    [
        "PUT /v1/devices/" ?TOKEN " HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\n\r\n",
        [[Size, "\r\n", Chunk, "\r\n"] || _ <- [1, 2]]
    ].

incomplete_body() ->
    "PUT /v1/devices/" ?TOKEN " HTTP/1.1\r\nhost: localhost\r\ncontent-length: 100\r\n\r\n{}".

raw(Port, Request) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, Request),
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),
    gen_tcp:close(Socket),
    [StatusLine | _] = binary:split(Response, <<"\r\n">>),
    [_Version, Status | _] = binary:split(StatusLine, <<" ">>, [global]),
    binary_to_integer(Status).

directory(Port) ->
    {200, Body} = request(get, Port, "/v1/directory"),
    ?assertEqual("no-cache", proplists:get_value("cache-control", headers(Port, "/v1/directory"))),
    {ok, MaxAge} = application:get_env(spacepush, max_data_age_s),
    ?assertEqual(integer_to_list(MaxAge),
        proplists:get_value("x-spacepush-max-data-age", headers(Port, "/v1/directory"))),
    Entries = json:decode(Body),
    %% Entries without a name are allowlisted but not listed.
    ?assertEqual(7, length(Entries)),
    [Nerd2Nerd] = [E || #{<<"url">> := ?NERD2NERD} = E <- Entries],
    ?assertMatch(#{<<"data">> := #{<<"space">> := <<"Nerd2Nerd">>, <<"location">> := #{<<"address">> := _}}}, Nerd2Nerd).

%% The summary counts the same states the directory lists, and any page may read it.
summary(Port) ->
    {200, Directory} = request(get, Port, "/v1/directory"),
    Entries = json:decode(Directory),
    Open = length([E || #{<<"data">> := #{<<"state">> := #{<<"open">> := true}}} = E <- Entries]),
    {200, Body} = request(get, Port, "/v1/summary"),
    ?assertMatch(#{<<"open">> := Open, <<"total">> := 7, <<"as_of">> := <<_/binary>>}, json:decode(Body)),
    Headers = headers(Port, "/v1/summary"),
    ?assertEqual("*", proplists:get_value("access-control-allow-origin", Headers)),
    {ok, MaxAge} = application:get_env(spacepush, max_data_age_s),
    ?assertEqual(integer_to_list(MaxAge), proplists:get_value("x-spacepush-max-data-age", Headers)),
    ?assertEqual("X-SpacePush-Max-Data-Age", proplists:get_value("access-control-expose-headers", Headers)),
    ?assertEqual("no-cache", proplists:get_value("cache-control", Headers)).

document_freshness_headers(Port) ->
    {ok, MaxAge} = application:get_env(spacepush, max_data_age_s),
    [begin
        #{fetched_at := FetchedAt} = spacepush_cache:lookup(Endpoint),
        Headers = headers(Port, Path),
        ?assertEqual("no-cache", proplists:get_value("cache-control", Headers)),
        ?assertEqual(integer_to_list(FetchedAt div 1000), proplists:get_value("x-spacepush-fetched-at", Headers)),
        ?assertEqual(integer_to_list(MaxAge), proplists:get_value("x-spacepush-max-data-age", Headers)),
        ?assertEqual(integer_to_list((FetchedAt + MaxAge * 1000) div 1000),
            proplists:get_value("x-spacepush-expires-at", Headers))
    end || {Endpoint, Path} <- document_paths()].

expired_documents_are_not_successful_reads(Port) ->
    check_fallback_documents(Port, -901000, fun(Path, _FetchedAt) ->
        {502, Body} = request(get, Port, Path),
        ?assertMatch(#{<<"error">> := <<"stale_data">>}, json:decode(Body)),
        ?assertEqual(undefined, proplists:get_value("x-spacepush-fetched-at", headers(Port, Path)))
    end).

future_documents_are_not_successful_reads(Port) ->
    check_fallback_documents(Port, 3600000, fun(Path, _FetchedAt) ->
        {502, Body} = request(get, Port, Path),
        ?assertMatch(#{<<"error">> := <<"stale_data">>}, json:decode(Body))
    end).

recent_fallback_preserves_original_fetch_time(Port) ->
    check_fallback_documents(Port, -120000, fun(Path, FetchedAt) ->
        ?assertEqual(200, status(request(get, Port, Path))),
        Headers = headers(Port, Path),
        ?assertEqual(integer_to_list(FetchedAt div 1000), proplists:get_value("x-spacepush-fetched-at", Headers)),
        ?assertEqual("900", proplists:get_value("x-spacepush-max-data-age", Headers))
    end).

document_paths() ->
    {ok, Mainframe} = application:get_env(spacepush, mainframe_url),
    [{?NERD2NERD, "/v1/spaces?endpoint=" ?NERD2NERD_QS},
        {list_to_binary(Mainframe), "/v1/mainframe/rooms"}].

check_fallback_documents(_Port, Offset, Check) ->
    {ok, OldMaxAge} = application:get_env(spacepush, max_data_age_s),
    PreviousResponses = persistent_term:get({spacepush_test_util, responses}),
    application:set_env(spacepush, max_data_age_s, 900),
    try
        [begin
            Original = spacepush_cache:lookup(Endpoint),
            FetchedAt = erlang:system_time(millisecond) + Offset,
            spacepush_test_util:fake_responses(PreviousResponses#{Endpoint => {error, timeout}}),
            ets:insert(spacepush_cache, {Endpoint, Original#{fetched_at := FetchedAt}}),
            try Check(Path, FetchedAt)
            after ets:insert(spacepush_cache, {Endpoint, Original}) end
        end || {Endpoint, Path} <- document_paths()]
    after
        spacepush_test_util:fake_responses(PreviousResponses),
        application:set_env(spacepush, max_data_age_s, OldMaxAge)
    end.

passes_documents_through_safely(Port) ->
    Headers = headers(Port, "/v1/spaces?endpoint=" ?NERD2NERD_QS),
    ?assertEqual("nosniff", proplists:get_value("x-content-type-options", Headers)),
    ?assertEqual("application/json", proplists:get_value("content-type", Headers)).

rate_limited_requests_do_not_tally_versions(Port) ->
    application:set_env(spacepush, read_rate_limit_per_minute, 1),
    application:set_env(spacepush, rate_limit_per_minute, 1),
    ets:delete_all_objects(spacepush_ratelimit),
    try
        Read = [reported_request(get, Port, "/v1/summary", N) || N <- [9001, 9002, 9003]],
        Write = [reported_request(put, Port, "/v1/devices/" ?TOKEN, N) || N <- [9011, 9012, 9013]],
        ?assertEqual([200, 429, 429], Read),
        ?assertEqual([204, 429, 429], Write),
        Report = spacepush_stats:report(1),
        Installs = maps:get(<<"installs">>, Report),
        ?assertEqual(2, maps:get(<<"this_week">>, Installs)),
        Versions = maps:get(<<"versions">>, maps:get(<<"this_week_by">>, Installs)),
        ?assertEqual(#{<<"9001">> => 1, <<"9011">> => 1}, Versions),
        [Today | _] = maps:get(<<"days">>, Report),
        ?assertMatch(#{<<"rate_limited.read">> := 2, <<"rate_limited.write">> := 2}, Today)
    after
        application:set_env(spacepush, read_rate_limit_per_minute, 300),
        application:set_env(spacepush, rate_limit_per_minute, 30)
    end.

reported_request(Method, Port, Path, Version) ->
    Headers = [
        {"user-agent", "SpaceState/" ++ integer_to_list(Version) ++ " (iOS 26.0)"},
        {"x-spacestate-first", "week"}
    ],
    Result = case Method of
        get -> httpc:request(get, {url(Port, Path), Headers}, [], [{body_format, binary}]);
        put -> httpc:request(put, {url(Port, Path), Headers, "application/json", registration(?NERD2NERD)}, [], [{body_format, binary}])
    end,
    status(reply(Result)).

test_push_endpoint(Port) ->
    ets:delete_all_objects(spacepush_ratelimit),
    Path = "/v1/test?endpoint=" ?NERD2NERD_QS,
    ?assertEqual(405, status(request(get, Port, Path))),
    ?assertEqual(400, status(request(post, Port, "/v1/test", <<>>))),
    ?assertEqual(400, status(request(post, Port, Path ++ "&room=%0A", <<>>))),
    ?assertEqual(404, status(request(post, Port, "/v1/test?endpoint=https%3A%2F%2Funknown.example", <<>>))),
    [begin
        Response = httpc:request(post, {url(Port, Path), [{Header, "127.0.0.1"}], "application/json", <<>>}, [], [{body_format, binary}]),
        ?assertEqual(404, status(reply(Response)))
    end || Header <- ["x-forwarded-for", "forwarded", "x-real-ip"]],
    Token = list_to_binary(?TOKEN),
    Second = binary:copy(<<"cd">>, 32),
    ok = spacepush_registry:register(Token, sandbox, [?TOPIC]),
    ok = spacepush_registry:register(Second, production, [?TOPIC]),
    ok = sys:suspend(spacepush_apns),
    try
        {202, Body} = request(post, Port, Path, <<>>),
        ?assertEqual(#{<<"recipients">> => 2, <<"test">> => true}, json:decode(Body)),
        Tests = [D || #{key := {_, ?TOPIC, test}} = D <- spacepush_outbox:due(erlang:system_time(millisecond), [], 100)],
        ?assertEqual(2, length(Tests)),
        ?assertEqual(429, status(request(post, Port, Path, <<>>)))
    after
        ok = spacepush_registry:unregister(Token),
        ok = spacepush_registry:unregister(Second),
        sys:resume(spacepush_apns)
    end.

stats_paths() ->
    ["/v1/stats", "/stats", "/stats/", "/stats/data.json", "/stats/dashboard.css", "/stats/dashboard.js"].

stats_login() ->
    [{"authorization", "Basic " ++ binary_to_list(base64:encode(<<"viewer:stats-test:password">>))}].

with_stats_login(Fun) ->
    OldUser = application:get_env(spacepush, stats_username, undefined),
    OldPassword = application:get_env(spacepush, stats_password, undefined),
    application:set_env(spacepush, stats_username, <<"viewer">>),
    application:set_env(spacepush, stats_password, <<"stats-test:password">>),
    try Fun()
    after
        application:set_env(spacepush, stats_username, OldUser),
        application:set_env(spacepush, stats_password, OldPassword)
    end.

stats_request(Method, Port, Path, Headers) ->
    Request = case Method of
        get -> {url(Port, Path), Headers};
        head -> {url(Port, Path), Headers};
        _ -> {url(Port, Path), Headers, "application/json", <<>>}
    end,
    {ok, {{_, Status, _}, ResponseHeaders, Body}} = httpc:request(Method, Request, [], [{body_format, binary}]),
    {Status, ResponseHeaders, Body}.

statistics_require_configured_credentials(Port) ->
    with_stats_login(fun() ->
        [begin
            application:set_env(spacepush, stats_username, User),
            application:set_env(spacepush, stats_password, Password),
            [begin
                {503, Headers, Body} = stats_request(get, Port, Path, stats_login()),
                ?assertEqual("no-store", proplists:get_value("cache-control", Headers)),
                ?assertEqual(#{<<"error">> => <<"stats_unavailable">>}, json:decode(Body))
            end || Path <- stats_paths()]
        end || {User, Password} <- [{undefined, undefined}, {<<"viewer">>, undefined},
            {undefined, <<"stats-test:password">>}, {<<>>, <<"password">>}, {<<"viewer">>, <<>>},
            {"viewer", "password"}, {<<"bad:user">>, <<"password">>}]]
    end).

statistics_reject_invalid_logins(Port) ->
    with_stats_login(fun() ->
        BadLogins = [[], [{"authorization", "Basic !!!"}], [{"authorization", "Bearer abc"}],
            [{"authorization", "Basic " ++ binary_to_list(base64:encode(<<"no-colon">>))}]],
        Wrong = [[{"authorization", "Basic " ++ binary_to_list(base64:encode(Value))}]
            || Value <- [<<"viewer:wrong">>, <<"wrong:stats-test:password">>, <<":">>]],
        [begin
            {401, Headers, Body} = stats_request(get, Port, Path, Login),
            ?assertEqual("Basic realm=\"SpacePush statistics\", charset=\"UTF-8\"",
                proplists:get_value("www-authenticate", Headers)),
            ?assertEqual("no-store", proplists:get_value("cache-control", Headers)),
            ?assertEqual(#{<<"error">> => <<"unauthorized">>}, json:decode(Body))
        end || Path <- stats_paths(), Login <- BadLogins ++ Wrong]
    end).

statistics_stay_hidden_through_proxies(Port) ->
    with_stats_login(fun() ->
        [begin
            {404, Headers, Body} = stats_request(get, Port, Path, [{Header, "127.0.0.1"} | stats_login()]),
            ?assertEqual(undefined, proplists:get_value("www-authenticate", Headers)),
            ?assertEqual(#{<<"error">> => <<"not_found">>}, json:decode(Body))
        end || Path <- stats_paths(), Header <- ["x-forwarded-for", "forwarded", "x-real-ip"]]
    end).

statistics_and_dashboard_work_with_login(Port) ->
    with_stats_login(fun() ->
        [begin
            {200, Headers, Body} = stats_request(get, Port, Path ++ "?days=3", stats_login()),
            ?assertEqual("no-store", proplists:get_value("cache-control", Headers)),
            ?assertEqual(undefined, proplists:get_value("access-control-allow-origin", Headers)),
            ?assertMatch(#{<<"days">> := [_, _, _], <<"service">> := #{<<"version">> := _}}, json:decode(Body))
        end || Path <- ["/v1/stats", "/stats/data.json"]],
        [begin
            {200, _, Body} = stats_request(get, Port, "/v1/stats?days=" ++ Days, stats_login()),
            ?assertEqual(Count, length(maps:get(<<"days">>, json:decode(Body))))
        end || {Days, Count} <- [{"garbage", 14}, {"0", 1}, {"900", 400}]],
        [begin
            {200, Headers, Body} = stats_request(get, Port, Path, stats_login()),
            ?assertEqual(Type, proplists:get_value("content-type", Headers)),
            ?assertEqual("no-store", proplists:get_value("cache-control", Headers)),
            ?assertEqual("DENY", proplists:get_value("x-frame-options", Headers)),
            ?assertEqual("nosniff", proplists:get_value("x-content-type-options", Headers)),
            ?assertNotEqual(undefined, proplists:get_value("content-security-policy", Headers)),
            ?assertNotEqual(nomatch, binary:match(Body, Marker))
        end || {Path, Type, Marker} <- [
            {"/stats/", "text/html; charset=utf-8", <<"/stats/dashboard.js">>},
            {"/stats/dashboard.css", "text/css; charset=utf-8", <<"--accent">>},
            {"/stats/dashboard.js", "text/javascript; charset=utf-8", <<"/stats/data.json">>}]],
        ?assertMatch({200, _, <<>>}, stats_request(head, Port, "/stats/", stats_login())),
        ?assertMatch({405, _, _}, stats_request(post, Port, "/stats/data.json", stats_login())),
        ?assertMatch({404, _, _}, stats_request(get, Port, "/stats/sys.config", stats_login())),
        ?assertMatch({404, _, _}, stats_request(get, Port, "/stats/%2e%2e/config/sys.config", stats_login()))
    end).
