-module(spacepush_http_devices_tests).
-include_lib("eunit/include/eunit.hrl").

-define(ENDPOINT, <<"https://status.mainframe.io/api/spaceInfo">>).

registration(Environment, Subscriptions) ->
    iolist_to_binary(json:encode(#{<<"environment">> => Environment, <<"subscriptions">> => Subscriptions})).

valid_registration_test() ->
    Body = registration(<<"sandbox">>, [
        #{<<"endpoint">> => ?ENDPOINT, <<"room">> => <<"radstelle">>},
        #{<<"endpoint">> => ?ENDPOINT}
    ]),
    ?assertEqual(
        {ok, sandbox, [{?ENDPOINT, <<"radstelle">>}, {?ENDPOINT, <<"space">>}]},
        spacepush_http_devices:parse_registration(Body)
    ).

duplicate_subscriptions_collapse_test() ->
    Body = registration(<<"production">>, [#{<<"endpoint">> => ?ENDPOINT}, #{<<"endpoint">> => ?ENDPOINT}]),
    ?assertEqual({ok, production, [{?ENDPOINT, <<"space">>}]}, spacepush_http_devices:parse_registration(Body)).

platform_test_() ->
    Body = fun(Platform) ->
        iolist_to_binary(json:encode(#{<<"environment">> => <<"sandbox">>, <<"subscriptions">> => [], <<"platform">> => Platform}))
    end,
    [
        ?_assertEqual({ok, sandbox, [], <<"ios">>}, spacepush_http_devices:registration(Body(<<"ios">>))),
        ?_assertEqual({ok, sandbox, [], <<"macos">>}, spacepush_http_devices:registration(Body(<<"macos">>))),
        ?_assertEqual({ok, sandbox, [], <<"unknown">>}, spacepush_http_devices:registration(registration(<<"sandbox">>, []))),
        ?_assertEqual({error, <<"invalid_platform">>}, spacepush_http_devices:registration(Body(<<"windows">>)))
    ].

invalid_registrations_test_() ->
    Cases = [
        {<<"invalid_json">>, <<"{nope">>},
        {<<"invalid_registration">>, <<"[]">>},
        {<<"invalid_environment">>, registration(<<"staging">>, [])},
        {<<"invalid_subscription">>, registration(<<"sandbox">>, [#{<<"endpoint">> => <<"ftp://a.example/">>}])},
        {<<"invalid_subscription">>, registration(<<"sandbox">>, [#{<<"endpoint">> => <<"not a url">>}])},
        {<<"invalid_subscription">>, registration(<<"sandbox">>, [#{<<"endpoint">> => ?ENDPOINT, <<"room">> => <<"../etc">>}])},
        {<<"invalid_subscription">>, registration(<<"sandbox">>, [#{<<"room">> => <<"space">>}])},
        {<<"invalid_registration">>, registration(<<"sandbox">>, lists:duplicate(51, #{<<"endpoint">> => ?ENDPOINT}))}
    ],
    [?_assertEqual({error, Reason}, spacepush_http_devices:parse_registration(Body)) || {Reason, Body} <- Cases].

valid_token_test_() ->
    Hex64 = binary:copy(<<"ab">>, 32),
    [
        ?_assert(spacepush_http_devices:valid_token(Hex64)),
        ?_assert(spacepush_http_devices:valid_token(string:uppercase(Hex64))),
        ?_assertNot(spacepush_http_devices:valid_token(binary:copy(<<"ab">>, 31))),
        ?_assertNot(spacepush_http_devices:valid_token(<<Hex64/binary, "zz">>)),
        ?_assertNot(spacepush_http_devices:valid_token(undefined)),
        ?_assert(spacepush_http_devices:valid_token(binary:copy(<<"a">>, 200))),
        ?_assertNot(spacepush_http_devices:valid_token(binary:copy(<<"a">>, 201)))
    ].

token_control_bytes_test_() ->
    Hex = binary:copy(<<"ab">>, 32),
    [
        ?_assertNot(spacepush_http_devices:valid_token(<<Hex/binary, Suffix/binary>>))
     || Suffix <- [<<10>>, <<13>>, <<0>>, <<" ">>, <<9>>, <<"/">>, <<"%0A">>, <<255>>]
    ].

invalid_rooms_test_() ->
    [
        ?_assertEqual(
            {error, <<"invalid_subscription">>},
            spacepush_http_devices:parse_registration(
                registration(<<"sandbox">>, [#{<<"endpoint">> => ?ENDPOINT, <<"room">> => Room}])
            )
        )
     || Room <- [<<"space\n">>, <<"space\r">>, <<"space", 0>>, <<"space ">>, <<"Space">>, <<>>, binary:copy(<<"a">>, 33)]
    ].

room_length_boundary_test() ->
    Room = binary:copy(<<"a">>, 32),
    ?assertEqual(
        {ok, sandbox, [{?ENDPOINT, Room}]},
        spacepush_http_devices:parse_registration(
            registration(<<"sandbox">>, [#{<<"endpoint">> => ?ENDPOINT, <<"room">> => Room}])
        )
    ).
