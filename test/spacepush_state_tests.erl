-module(spacepush_state_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MAINFRAME, <<"https://status.mainframe.io/api/spaceInfo">>).
%% lastSeen of the fixture entries is 1791485450.
-define(NOW, 1791485510).
-define(MAX_AGE, 900).
-define(DEBOUNCE, 120).

parse(Body) -> spacepush_state:parse_aggregator(Body, ?NOW, ?MAX_AGE).

states(Observations) ->
    maps:from_list([{Name, State} || {_Topic, Name, State} <- Observations]).

%% Parsing

aggregator_test() ->
    Observations = parse(spacepush_test_util:fixture("aggregator.json")),
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
    ?assertNot(lists:keymember(<<"Mainframe">>, 2, parse(spacepush_test_util:fixture("aggregator.json")))).

aggregator_skips_broken_entries_test() ->
    Body = <<"[42, {\"url\": \"https://a.example/\", \"lastSeen\": 1791485450, \"data\": {\"space\": \"A\"}},"
             " {\"url\": 1}, {\"data\": null}]">>,
    ?assertEqual([{{<<"https://a.example/">>, <<"space">>}, <<"A">>, unknown}], parse(Body)).

aggregator_accepts_items_object_test() ->
    Body = <<"{\"items\": [{\"url\": \"https://a.example/\", \"lastSeen\": 1791485450,"
             " \"data\": {\"space\": \"A\", \"state\": {\"open\": true}}}]}">>,
    ?assertEqual([{{<<"https://a.example/">>, <<"space">>}, <<"A">>, open}], parse(Body)).

entry(Extra) ->
    Entry = maps:merge(
        #{
            <<"url">> => <<"https://a.example/">>,
            <<"lastSeen">> => ?NOW - 60,
            <<"data">> => #{<<"space">> => <<"A">>, <<"state">> => #{<<"open">> => true}}
        },
        Extra
    ),
    [{_Topic, _Name, State}] = parse(iolist_to_binary(json:encode([Entry]))),
    State.

fresh_data_counts_test() ->
    ?assertEqual(open, entry(#{})).

stale_data_is_unknown_test() ->
    ?assertEqual(unknown, entry(#{<<"lastSeen">> => 1})).

missing_last_seen_is_unknown_test() ->
    ?assertEqual(unknown, entry(#{<<"lastSeen">> => null})).

unreachable_endpoint_is_unknown_test() ->
    ?assertEqual(unknown, entry(#{<<"validationResult">> => #{<<"reachable">> => false}})).

invalid_schema_still_counts_test() ->
    ?assertEqual(open, entry(#{<<"valid">> => false, <<"validationResult">> => #{<<"reachable">> => true}})).

mainframe_test() ->
    Observations = spacepush_state:parse_mainframe(spacepush_test_util:fixture("mainframe-openstate.json")),
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

%% Tracking

-define(TOPIC, {<<"https://a.example/">>, <<"space">>}).

%% Runs polls given as [{Second, State}] and returns the changes with the second they were reported.
run(Polls) ->
    {_Tracker, Reported} = lists:foldl(
        fun({Second, State}, {Tracker, Acc}) ->
            {Tracker1, Changes} = spacepush_state:track(Tracker, [{?TOPIC, <<"A">>, State}], Second, ?DEBOUNCE),
            {Tracker1, Acc ++ [{Second, From, To} || {_, _, From, To} <- Changes]}
        end,
        {#{}, []},
        Polls
    ),
    Reported.

first_observation_is_no_change_test() ->
    ?assertEqual([], run([{0, open}])).

change_needs_debounce_test() ->
    ?assertEqual(
        [{240, open, closed}],
        run([{0, open}, {120, closed}, {180, closed}, {240, closed}, {300, closed}])
    ).

flap_back_sends_nothing_test() ->
    ?assertEqual([], run([{0, open}, {60, closed}, {120, open}, {180, open}, {240, open}])).

%% closed -> keyholder at 60, keyholder -> member at 120: member must hold its own two minutes.
new_candidate_restarts_wait_test() ->
    ?assertEqual(
        [{240, closed, member}],
        run([{0, closed}, {60, keyholder}, {120, member}, {180, member}, {240, member}])
    ).

unknown_cannot_confirm_test() ->
    ?assertEqual(
        [{300, open, closed}],
        run([{0, open}, {60, closed}, {120, unknown}, {180, unknown}, {240, unknown}, {300, closed}])
    ).

unknown_keeps_confirmed_state_test() ->
    ?assertEqual([], run([{0, open}, {60, unknown}, {120, open}, {300, open}])).

tracker_survives_restart_test() ->
    Dir = spacepush_test_util:tmp_dir("tracker"),
    File = filename:join(Dir, "tracker.bin"),
    {Tracker, []} = spacepush_state:track(#{}, [{?TOPIC, <<"A">>, open}], 0, ?DEBOUNCE),
    {Tracker1, []} = spacepush_state:track(Tracker, [{?TOPIC, <<"A">>, closed}], 60, ?DEBOUNCE),
    ok = spacepush_store:save(File, spacepush_state:snapshot(Tracker1)),
    Restored = spacepush_state:restore(spacepush_store:load(File, none)),
    ?assertMatch({_, [{?TOPIC, _, open, closed}]}, spacepush_state:track(Restored, [{?TOPIC, <<"A">>, closed}], 180, ?DEBOUNCE)).

restore_keeps_valid_entries_test() ->
    Good = #{confirmed => open, candidate => {closed, 60}},
    Tracker = #{?TOPIC => Good, {<<"https://b.example/">>, <<"space">>} => #{confirmed => party, candidate => none}, bad => Good},
    ?assertEqual(#{?TOPIC => Good}, spacepush_state:restore(spacepush_state:snapshot(Tracker))).

restore_rejects_unknown_formats_test_() ->
    [?_assertEqual(#{}, spacepush_state:restore(Term)) || Term <- [none, #{}, {tracker, 2, #{}}, {tracker, 1, []}]].
