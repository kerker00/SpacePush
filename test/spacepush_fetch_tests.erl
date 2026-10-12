-module(spacepush_fetch_tests).
-include_lib("eunit/include/eunit.hrl").

url(N) -> <<"https://s", (integer_to_binary(N))/binary, ".example/">>.

fetch_test_() ->
    {setup, fun() -> spacepush_test_util:setup_env("fetch") end, [
        fun returns_every_result/0,
        {timeout, 10, fun respects_concurrency/0}
    ]}.

returns_every_result() ->
    spacepush_test_util:fake_responses(#{url(1) => {ok, <<"one">>}, url(2) => {error, timeout}}),
    ?assertEqual(
        #{url(1) => {ok, <<"one">>}, url(2) => {error, timeout}, url(3) => {error, not_found}},
        spacepush_fetch:many([url(1), url(2), url(3)], 2)
    ).

%% 12 requests of 100 ms with at most 4 in parallel need three rounds.
respects_concurrency() ->
    Urls = [url(N) || N <- lists:seq(1, 12)],
    spacepush_test_util:fake_responses(maps:from_list([{U, {delay, 100, {ok, <<"x">>}}} || U <- Urls])),
    {Micros, Results} = timer:tc(fun() -> spacepush_fetch:many(Urls, 4) end),
    ?assertEqual(12, map_size(Results)),
    ?assert(Micros >= 300000),
    ?assert(Micros < 1000000).

public_address_test_() ->
    [
        ?_assertEqual(Expected, spacepush_fetch:public_address(Address))
     || {Address, Expected} <- [
            {{93, 184, 216, 34}, true},
            {{127, 0, 0, 1}, false},
            {{10, 1, 2, 3}, false},
            {{172, 16, 0, 1}, false},
            {{172, 32, 0, 1}, true},
            {{192, 168, 1, 1}, false},
            {{169, 254, 169, 254}, false},
            {{100, 64, 0, 1}, false},
            {{0, 0, 0, 0}, false},
            {{224, 0, 0, 1}, false},
            {{255, 255, 255, 255}, false},
            {{16#2a01, 16#4f8, 0, 0, 0, 0, 0, 1}, true},
            {{0, 0, 0, 0, 0, 0, 0, 1}, false},
            {{0, 0, 0, 0, 0, 0, 0, 0}, false},
            {{16#fd00, 0, 0, 0, 0, 0, 0, 1}, false},
            {{16#fe80, 0, 0, 0, 0, 0, 0, 1}, false},
            {{16#ff02, 0, 0, 0, 0, 0, 0, 1}, false},
            {{0, 0, 0, 0, 0, 16#ffff, 16#7f00, 1}, false},
            {{0, 0, 0, 0, 0, 16#ffff, 16#5db8, 16#d822}, true}
        ]
    ].

%% These are refused before any connection is made, so they need no network.
refuses_internal_targets_test_() ->
    {setup, fun() -> spacepush_test_util:setup_env("fetch_internal") end, [
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("http://127.0.0.1:8080/v1/directory", 1000)),
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("https://[::1]/", 1000)),
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("http://169.254.169.254/latest/meta-data/", 1000)),
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("http://localhost/", 1000)),
        ?_assertEqual({error, invalid_url}, spacepush_fetch:http_get("file:///etc/passwd", 1000)),
        ?_assertEqual({error, invalid_url}, spacepush_fetch:http_get("gopher://example.org/", 1000))
    ]}.

size_limit_applies_test() ->
    spacepush_test_util:setup_env("fetch_limit"),
    Big = binary:copy(<<"x">>, 300000),
    spacepush_test_util:fake_responses(#{url(1) => {ok, Big}}),
    ?assertEqual({error, too_large}, spacepush_fetch:get(url(1))),
    ?assertEqual({ok, Big}, spacepush_fetch:get(url(1), 4194304)).

informational_responses_test_() ->
    [?_test(begin
        Conn = spawn(fun() -> receive stop -> ok end end),
        Ref = make_ref(),
        try
            self() ! {gun_inform, Conn, Ref, Status, []},
            self() ! {gun_response, Conn, Ref, nofin, 200, []},
            self() ! {gun_data, Conn, Ref, fin, <<"ok">>},
            Deadline = erlang:monotonic_time(millisecond) + 1000,
            ?assertEqual({ok, <<"ok">>}, spacepush_fetch:response(Conn, Ref, Deadline, 100))
        after Conn ! stop end
    end) || Status <- [100, 103]].

informational_response_does_not_reset_deadline_test() ->
    Conn = spawn(fun() -> receive stop -> ok end end),
    Ref = make_ref(),
    try
        self() ! {gun_inform, Conn, Ref, 103, []},
        Started = erlang:monotonic_time(millisecond),
        ?assertEqual({error, timeout}, spacepush_fetch:response(Conn, Ref, Started + 50, 100)),
        ?assert(erlang:monotonic_time(millisecond) - Started < 500)
    after Conn ! stop end.

expired_deadline_rejects_queued_informational_response_test() ->
    Conn = self(),
    Ref = make_ref(),
    self() ! {gun_inform, Conn, Ref, 103, []},
    Deadline = erlang:monotonic_time(millisecond) - 1,
    ?assertEqual({error, timeout}, spacepush_fetch:response(Conn, Ref, Deadline, 100)),
    receive {gun_inform, Conn, Ref, _, _} -> ok end.

total_fetch_deadline_test() ->
    spacepush_test_util:setup_env("fetch_deadline"),
    {ok, Previous} = application:get_env(spacepush, fetch_timeout_ms),
    application:set_env(spacepush, fetch_timeout_ms, 50),
    spacepush_test_util:fake_responses(#{url(1) => {notify, self(), {wait, {ok, <<"late">>}}}}),
    try
        Started = erlang:monotonic_time(millisecond),
        ?assertEqual({error, timeout}, spacepush_fetch:get(url(1))),
        ?assert(erlang:monotonic_time(millisecond) - Started < 500),
        Worker = receive {fetch_worker, Pid} -> Pid after 1000 -> error(no_worker) end,
        Ref = monitor(process, Worker),
        receive {'DOWN', Ref, process, Worker, _} -> ok after 1000 -> error(worker_survived_deadline) end
    after application:set_env(spacepush, fetch_timeout_ms, Previous) end.

transport_exception_is_a_result_test() ->
    spacepush_test_util:setup_env("fetch_exception"),
    spacepush_test_util:fake_responses(#{url(1) => {crash, test_transport_failure}}),
    ?assertMatch({error, {crashed, _}}, spacepush_fetch:get(url(1))).
