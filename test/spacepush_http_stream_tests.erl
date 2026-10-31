-module(spacepush_http_stream_tests).
-moduledoc "Starts the application with the stream listener and reads raw server-sent events.".
-include_lib("eunit/include/eunit.hrl").

-define(NERD2NERD, <<"https://api.nerd2nerd.org/status.json">>).
-define(NERD2NERD_QS, "https%3A%2F%2Fapi.nerd2nerd.org%2Fstatus.json").
-define(TOPIC, {?NERD2NERD, <<"space">>}).

subscriptions_test_() ->
    {setup, fun() -> application:load(spacepush), application:set_env(spacepush, stream_max_topics, 2) end, [
        ?_assertEqual({ok, [?TOPIC]}, spacepush_http_stream:subscriptions([{<<"sub">>, ?NERD2NERD}])),
        ?_assertEqual({ok, [?TOPIC]}, spacepush_http_stream:subscriptions([{<<"sub">>, ?NERD2NERD}, {<<"sub">>, <<?NERD2NERD/binary, "|space">>}])),
        ?_assertEqual({ok, [{<<"https://m.example/">>, <<"radstelle">>}]},
            spacepush_http_stream:subscriptions([{<<"sub">>, <<"https://m.example/|radstelle">>}, {<<"other">>, true}])),
        ?_assertEqual(error, spacepush_http_stream:subscriptions([])),
        ?_assertEqual(error, spacepush_http_stream:subscriptions([{<<"sub">>, true}])),
        ?_assertEqual(error, spacepush_http_stream:subscriptions([{<<"sub">>, <<>>}])),
        ?_assertEqual(error, spacepush_http_stream:subscriptions([{<<"sub">>, <<"|space">>}])),
        ?_assertEqual(error, spacepush_http_stream:subscriptions([{<<"sub">>, <<"https://a.example/|Kitchen">>}])),
        ?_assertEqual(error, spacepush_http_stream:subscriptions([{<<"sub">>, binary:copy(<<"a">>, 4097)}])),
        ?_assertEqual(error, spacepush_http_stream:subscriptions([{<<"sub">>, <<"a">>}, {<<"sub">>, <<"b">>}, {<<"sub">>, <<"c">>}]))
    ]}.

stream_test_() ->
    {setup, fun start/0, fun stop/1, fun(Ports) ->
        [
            ?_test(rejects_invalid_requests(Ports)),
            ?_test(streams_changes(Ports)),
            ?_test(sends_heartbeats(Ports)),
            ?_test(ends_after_lifetime(Ports)),
            ?_test(limits_streams_per_client(Ports)),
            ?_test(scope_restart_ends_streams(Ports)),
            ?_test(api_listener_has_no_stream(Ports))
        ]
    end}.

start() ->
    spacepush_test_util:setup_env("http_stream"),
    application:set_env(spacepush, http_port, 0),
    application:set_env(spacepush, stream_http_port, 0),
    application:set_env(spacepush, max_data_age_s, 1000000000),
    {ok, Aggregator} = application:get_env(spacepush, aggregator_url),
    {ok, Mainframe} = application:get_env(spacepush, mainframe_url),
    spacepush_test_util:fake_responses(#{
        list_to_binary(Aggregator) => {ok, spacepush_test_util:fixture("aggregator.json")},
        list_to_binary(Mainframe) => {ok, spacepush_test_util:fixture("mainframe-openstate.json")}
    }),
    {ok, _} = application:ensure_all_started(spacepush),
    sys:get_state(spacepush_directory),
    {ranch:get_port(spacepush_http), ranch:get_port(spacepush_stream_http)}.

stop(_Ports) ->
    ok = application:stop(spacepush),
    application:set_env(spacepush, stream_http_port, undefined).

rejects_invalid_requests({_Api, Port}) ->
    ?assertMatch({400, _}, request(Port, "GET", "/v1/stream")),
    ?assertMatch({400, _}, request(Port, "GET", "/v1/stream?sub=" ?NERD2NERD_QS "%7CKitchen")),
    ?assertMatch({404, _}, request(Port, "GET", "/v1/stream?sub=https%3A%2F%2Fevil.example%2F")),
    ?assertMatch({405, _}, request(Port, "POST", "/v1/stream?sub=" ?NERD2NERD_QS)).

streams_changes({_Api, Port}) ->
    Socket = open(Port),
    {Head, _} = wait(Socket, <<"retry: 10000\n\n">>),
    ?assertMatch({match, _}, re:run(Head, "HTTP/1.1 200")),
    ?assertMatch({match, _}, re:run(Head, "content-type: text/event-stream")),
    ?assertMatch({match, _}, re:run(Head, "cache-control: no-store")),
    ok = spacepush_stream:broadcast([{{<<"https://other.example/">>, <<"space">>}, <<"O">>, closed, open},
        {?TOPIC, <<"Nerd2Nerd">>, closed, open}]),
    {Data, _} = wait(Socket, <<"\n\n">>),
    [_, Json] = binary:split(Data, <<"event: change\ndata: ">>),
    ?assertMatch(#{<<"endpoint">> := ?NERD2NERD, <<"name">> := <<"Nerd2Nerd">>, <<"to">> := <<"open">>}, json:decode(Json)),
    gen_tcp:close(Socket).

sends_heartbeats({_Api, Port}) ->
    with_env(stream_heartbeat_ms, 100, fun() ->
        Socket = open(Port),
        ?assertMatch({_, _}, wait(Socket, <<": ping\n\n">>)),
        gen_tcp:close(Socket)
    end).

ends_after_lifetime({_Api, Port}) ->
    with_env(stream_max_lifetime_ms, 200, fun() ->
        Socket = open(Port),
        %% The last chunk of a chunked response.
        ?assertMatch({_, _}, wait(Socket, <<"\r\n0\r\n\r\n">>)),
        gen_tcp:close(Socket)
    end).

limits_streams_per_client({_Api, Port}) ->
    with_env(stream_max_per_client, 1, fun() ->
        Socket = open(Port),
        wait(Socket, <<"retry:">>),
        ?assertMatch({429, _}, request(Port, "GET", "/v1/stream?sub=" ?NERD2NERD_QS)),
        gen_tcp:close(Socket)
    end).

scope_restart_ends_streams({_Api, Port}) ->
    Socket = open(Port),
    wait(Socket, <<"retry:">>),
    Before = [Pid || {Id, Pid, _, _} <- supervisor:which_children(spacepush_sup), Id =/= spacepush_stream],
    exit(whereis(spacepush_stream), kill),
    %% The last chunk; the preceding chunk's CRLF was consumed above.
    ?assertMatch({_, _}, wait(Socket, <<"0\r\n\r\n">>)),
    gen_tcp:close(Socket),
    %% Only the scope itself restarts; the APNs pipeline keeps its processes.
    ?assertEqual(Before, [Pid || {Id, Pid, _, _} <- supervisor:which_children(spacepush_sup), Id =/= spacepush_stream]).

api_listener_has_no_stream({Api, _Port}) ->
    ?assertMatch({404, _}, request(Api, "GET", "/v1/stream?sub=" ?NERD2NERD_QS)).

with_env(Key, Value, Fun) ->
    {ok, Previous} = application:get_env(spacepush, Key),
    application:set_env(spacepush, Key, Value),
    try Fun() after application:set_env(spacepush, Key, Previous) end.

open(Port) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, ["GET /v1/stream?sub=", ?NERD2NERD_QS, " HTTP/1.1\r\nHost: localhost\r\n\r\n"]),
    Socket.

%% Reads until Pattern arrives; returns what came before it and the rest.
wait(Socket, Pattern) ->
    wait(Socket, Pattern, <<>>).

wait(Socket, Pattern, Buffer) ->
    case binary:split(Buffer, Pattern) of
        [Before, After] -> {Before, After};
        [_] ->
            {ok, Data} = gen_tcp:recv(Socket, 0, 2000),
            wait(Socket, Pattern, <<Buffer/binary, Data/binary>>)
    end.

request(Port, Method, Path) ->
    request(Port, Method, Path, "").

request(Port, Method, Path, Headers) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, [Method, " ", Path, " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n",
        "Content-Length: 0\r\n", Headers, "\r\n"]),
    Response = recv_all(Socket, <<>>),
    gen_tcp:close(Socket),
    [Head, Body] = binary:split(Response, <<"\r\n\r\n">>),
    {match, [Status]} = re:run(Head, "^HTTP/1.1 (\\d+)", [{capture, all_but_first, binary}]),
    {binary_to_integer(Status), Body}.

recv_all(Socket, Acc) ->
    case gen_tcp:recv(Socket, 0, 2000) of
        {ok, Data} -> recv_all(Socket, <<Acc/binary, Data/binary>>);
        {error, closed} -> Acc
    end.
