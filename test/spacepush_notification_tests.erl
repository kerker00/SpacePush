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

collapse_id_test() ->
    Id = spacepush_notification:collapse_id({?MAINFRAME, <<"space">>}),
    ?assertEqual(64, byte_size(Id)),
    ?assertEqual(Id, spacepush_notification:collapse_id({?MAINFRAME, <<"space">>})),
    ?assertNotEqual(Id, spacepush_notification:collapse_id({?MAINFRAME, <<"radstelle">>})).

title(Name) ->
    #{<<"aps">> := #{<<"alert">> := #{<<"title">> := Title}}} =
        spacepush_notification:payload({<<"https://a.example/">>, <<"space">>}, Name, open),
    Title.

title_drops_control_characters_test() ->
    ?assertEqual(<<"EvilSpace">>, title(<<"Evil\nSpace\r\t", 0>>)),
    ?assertEqual(<<"abc">>, title(<<"a‮b​c"/utf8>>)).

title_is_shortened_test() ->
    Title = title(binary:copy(<<"x">>, 500)),
    ?assertEqual(64, string:length(Title)),
    ?assertMatch(<<_:63/binary, "…"/utf8>>, Title).
