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
            ?_test(passes_documents_through_safely(Port))
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
    ?assertEqual("*", proplists:get_value("access-control-allow-origin", headers(Port, "/v1/summary"))).

passes_documents_through_safely(Port) ->
    Headers = headers(Port, "/v1/spaces?endpoint=" ?NERD2NERD_QS),
    ?assertEqual("nosniff", proplists:get_value("x-content-type-options", Headers)),
    ?assertEqual("application/json", proplists:get_value("content-type", Headers)).
