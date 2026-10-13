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
            spacepush_stats
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
