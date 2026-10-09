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
            {403, <<"InvalidProviderToken">>, renew_token},
            {403, <<"BadCertificate">>, drop},
            {429, <<"TooManyRequests">>, retry},
            {500, <<"InternalServerError">>, retry},
            {503, <<"ServiceUnavailable">>, retry}
        ]
    ].

backoff_grows_and_is_capped_test() ->
    ?assertEqual([5000, 10000, 20000, 40000], [spacepush_apns:backoff_ms(N) || N <- [0, 1, 2, 3]]),
    ?assertEqual(300000, spacepush_apns:backoff_ms(10)),
    ?assertEqual(300000, spacepush_apns:backoff_ms(1000)).
