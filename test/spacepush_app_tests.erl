-module(spacepush_app_tests).
-moduledoc "Starts the whole application and talks to its HTTP API.".
-include_lib("eunit/include/eunit.hrl").

-define(TOKEN, "abababababababababababababababababababababababababababababababab").
-define(TOPIC, {<<"https://a.example/">>, <<"space">>}).

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
            ?_assertEqual(405, status(request(get, Port, "/v1/devices/" ?TOKEN)))
        ]
    end}.

start() ->
    spacepush_test_util:setup_env("app"),
    application:set_env(spacepush, http_port, 0),
    application:set_env(spacepush, http_body_timeout_ms, 500),
    %% Nothing listens on port 9, so polling fails fast without network access.
    application:set_env(spacepush, aggregator_url, "http://127.0.0.1:9/"),
    application:set_env(spacepush, mainframe_url, "http://127.0.0.1:9/"),
    {ok, _} = application:ensure_all_started(spacepush),
    ranch:get_port(spacepush_http).

stop(_Port) ->
    ok = application:stop(spacepush).

all_children_running() ->
    Children = supervisor:which_children(spacepush_sup),
    ?assertEqual(
        [spacepush_ratelimit, spacepush_registry, spacepush_outbox, spacepush_apns, spacepush_poller],
        lists:reverse([Id || {Id, Pid, worker, _} <- Children, is_pid(Pid)])
    ).

register_and_unregister(Port) ->
    Body = iolist_to_binary(json:encode(#{
        <<"environment">> => <<"sandbox">>,
        <<"subscriptions">> => [#{<<"endpoint">> => <<"https://a.example/">>}]
    })),
    ?assertEqual(204, status(request(put, Port, "/v1/devices/" ?TOKEN, Body))),
    ?assertMatch([{_, sandbox, _}], spacepush_registry:subscribers(?TOPIC)),
    ?assertEqual(204, status(request(delete, Port, "/v1/devices/" ?TOKEN))),
    ?assertEqual([], spacepush_registry:subscribers(?TOPIC)).

url(Port, Path) ->
    "http://127.0.0.1:" ++ integer_to_list(Port) ++ Path.

request(Method, Port, Path) ->
    reply(httpc:request(Method, {url(Port, Path), []}, [], [{body_format, binary}])).

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
