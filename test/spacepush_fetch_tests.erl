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
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("http://127.0.0.1:8080/v1/directory")),
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("https://[::1]/")),
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("http://169.254.169.254/latest/meta-data/")),
        ?_assertEqual({error, non_public_address}, spacepush_fetch:http_get("http://localhost/")),
        ?_assertEqual({error, invalid_url}, spacepush_fetch:http_get("file:///etc/passwd")),
        ?_assertEqual({error, invalid_url}, spacepush_fetch:http_get("gopher://example.org/"))
    ]}.
