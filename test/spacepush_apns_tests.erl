-module(spacepush_apns_tests).
-include_lib("eunit/include/eunit.hrl").

classify_test_() ->
    [
        ?_assertEqual(Expected, spacepush_apns:classify(Status, Reason))
     || {Status, Reason, Expected} <- [
            {200, undefined, delivered},
            {410, <<"Unregistered">>, invalid_token},
            {400, <<"BadDeviceToken">>, invalid_token},
            {400, <<"DeviceTokenNotForTopic">>, invalid_token},
            {400, <<"PayloadEmpty">>, drop},
            {413, <<"PayloadTooLarge">>, drop},
            {403, <<"ExpiredProviderToken">>, renew_token},
            {403, <<"InvalidProviderToken">>, invalid_provider_token},
            {429, <<"TooManyProviderTokenUpdates">>, retry},
            {403, <<"BadCertificate">>, drop},
            {429, <<"TooManyRequests">>, retry},
            {500, <<"InternalServerError">>, retry},
            {503, <<"ServiceUnavailable">>, retry}
        ]
    ].

key_config_test_() ->
    Both = #{sandbox => {"s.p8", <<"S">>}, production => {"p.p8", <<"P">>}},
    [
        ?_assertEqual(Both, spacepush_apns:key_config(Both, "shared.p8", <<"X">>)),
        ?_assertEqual(#{sandbox => {"s.p8", <<"S">>}}, spacepush_apns:key_config(#{sandbox => {"s.p8", <<"S">>}, staging => x}, undefined, undefined)),
        ?_assertEqual(
            #{sandbox => {"shared.p8", <<"X">>}, production => {"shared.p8", <<"X">>}},
            spacepush_apns:key_config(undefined, "shared.p8", <<"X">>)
        ),
        ?_assertEqual(#{}, spacepush_apns:key_config(undefined, undefined, undefined))
    ].

token_usable_test_() ->
    Minute = 60,
    [
        ?_assert(spacepush_apns:token_usable(0, 49 * Minute, false)),
        ?_assertNot(spacepush_apns:token_usable(0, 50 * Minute, false)),
        %% After ExpiredProviderToken a token is replaced only once it is 20 minutes old.
        ?_assert(spacepush_apns:token_usable(0, 19 * Minute, true)),
        ?_assertNot(spacepush_apns:token_usable(0, 20 * Minute, true))
    ].

backoff_grows_and_is_capped_test() ->
    ?assertEqual([5000, 10000, 20000, 40000], [spacepush_apns:backoff_ms(N) || N <- [0, 1, 2, 3]]),
    ?assertEqual(300000, spacepush_apns:backoff_ms(10)),
    ?assertEqual(300000, spacepush_apns:backoff_ms(1000)).
