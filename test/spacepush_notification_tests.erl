-module(spacepush_notification_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MAINFRAME, <<"https://status.mainframe.io/api/spaceInfo">>).

space_payload_test() ->
    Payload = spacepush_notification:payload({<<"https://a.example/">>, <<"space">>}, <<"A">>, open),
    ?assertMatch(
        #{
            <<"aps">> := #{
                <<"alert">> := #{<<"title">> := <<"A">>, <<"loc-key">> := <<"PUSH_STATE_OPEN">>},
                <<"thread-id">> := <<"https://a.example/">>
            },
            <<"endpoint">> := <<"https://a.example/">>,
            <<"room">> := <<"space">>,
            <<"state">> := <<"open">>
        },
        Payload
    ).

room_title_test() ->
    #{<<"aps">> := #{<<"alert">> := #{<<"title">> := Title}}} =
        spacepush_notification:payload({?MAINFRAME, <<"radstelle">>}, <<"Mainframe">>, member),
    ?assertEqual(<<"Mainframe · Radstelle"/utf8>>, Title).

payload_encodes_as_json_test() ->
    Payload = spacepush_notification:payload({?MAINFRAME, <<"lab3d">>}, <<"Mainframe">>, open_plus),
    ?assertEqual(Payload, json:decode(iolist_to_binary(json:encode(Payload)))).

every_known_state_has_a_key_test_() ->
    [?_assertMatch(<<"PUSH_STATE_", _/binary>>, spacepush_notification:loc_key(State))
     || State <- [open, closed, keyholder, member, open_plus, closing]].
