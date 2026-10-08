-module(spacepush_state_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MAINFRAME, <<"https://status.mainframe.io/api/spaceInfo">>).

fixture(Name) ->
    {ok, Body} = file:read_file(filename:join([filename:dirname(?FILE), "fixtures", Name])),
    Body.

states(Observations) ->
    maps:from_list([{Name, State} || {_Topic, Name, State} <- Observations]).

aggregator_test() ->
    Observations = spacepush_state:parse_aggregator(fixture("aggregator.json")),
    ?assertEqual(
        #{
            <<"Nerd2Nerd">> => open,
            <<"Hacker Embassy">> => closed,
            <<"Apollo-NG">> => closed,
            <<"B4CKSP4CE">> => unknown,
            <<"LuXeria">> => unknown,
            <<"Milton Keynes Makerspace">> => unknown
        },
        states(Observations)
    ),
    ?assert(lists:all(fun({{_Url, Room}, _, _}) -> Room =:= <<"space">> end, Observations)).

aggregator_skips_mainframe_test() ->
    Observations = spacepush_state:parse_aggregator(fixture("aggregator.json")),
    ?assertNot(lists:keymember(<<"Mainframe">>, 2, Observations)).

aggregator_skips_broken_entries_test() ->
    Body = <<"[42, {\"url\": \"https://a.example/\", \"data\": {\"space\": \"A\"}}, {\"url\": 1}, {\"data\": null}]">>,
    ?assertEqual([{{<<"https://a.example/">>, <<"space">>}, <<"A">>, unknown}], spacepush_state:parse_aggregator(Body)).

mainframe_test() ->
    Observations = spacepush_state:parse_mainframe(fixture("mainframe-openstate.json")),
    ?assertEqual(
        lists:sort([
            {{?MAINFRAME, <<"space">>}, <<"Mainframe">>, open_plus},
            {{?MAINFRAME, <<"radstelle">>}, <<"Mainframe">>, closed},
            {{?MAINFRAME, <<"lab3d">>}, <<"Mainframe">>, closed},
            {{?MAINFRAME, <<"machining">>}, <<"Mainframe">>, closed}
        ]),
        lists:sort(Observations)
    ).

mainframe_state_test_() ->
    [
        ?_assertEqual(Expected, spacepush_state:mainframe_state(Raw))
     || {Raw, Expected} <- [
            {<<"none">>, closed},
            {<<"off">>, closed},
            {<<"keyholder">>, keyholder},
            {<<"member">>, member},
            {<<"open">>, open},
            {<<"on">>, open},
            {<<"open+">>, open_plus},
            {<<"closing">>, closing},
            {<<"party">>, unknown}
        ]
    ].

topic(Name) -> {<<"https://", Name/binary>>, <<"space">>}.

first_observation_is_no_change_test() ->
    ?assertEqual(
        {#{topic(<<"a">>) => open}, []},
        spacepush_state:changes(#{}, [{topic(<<"a">>), <<"A">>, open}])
    ).

change_is_reported_test() ->
    ?assertEqual(
        {#{topic(<<"a">>) => closed}, [{topic(<<"a">>), <<"A">>, open, closed}]},
        spacepush_state:changes(#{topic(<<"a">>) => open}, [{topic(<<"a">>), <<"A">>, closed}])
    ).

same_state_is_no_change_test() ->
    Known = #{topic(<<"a">>) => open},
    ?assertEqual({Known, []}, spacepush_state:changes(Known, [{topic(<<"a">>), <<"A">>, open}])).

unknown_keeps_last_known_state_test() ->
    Known = #{topic(<<"a">>) => open},
    {Known1, Changes1} = spacepush_state:changes(Known, [{topic(<<"a">>), <<"A">>, unknown}]),
    ?assertEqual({Known, []}, {Known1, Changes1}),
    {_, Changes2} = spacepush_state:changes(Known1, [{topic(<<"a">>), <<"A">>, closed}]),
    ?assertEqual([{topic(<<"a">>), <<"A">>, open, closed}], Changes2).
