-module(spacepush_state_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MAINFRAME, <<"https://status.mainframe.io/api/spaceInfo">>).
%% lastSeen of the fixture entries is 1791485450.
-define(NOW, 1791485510).
-define(MAX_AGE, 900).
-define(DEBOUNCE, 120).

directory(Body) -> spacepush_state:parse_directory(Body, ?NOW, ?MAX_AGE).

by_name(Entries) ->
    maps:from_list([{Name, Entry} || #{name := Name} = Entry <- Entries]).

%% Directory

directory_keeps_every_listed_endpoint_test() ->
    Entries = directory(spacepush_test_util:fixture("aggregator.json")),
    ?assertEqual(8, length(Entries)),
    Endpoints = [Endpoint || #{endpoint := Endpoint} <- Entries],
    ?assert(lists:member(?MAINFRAME, Endpoints)),
    %% Listed without data: allowed, but nothing to show.
    ?assertMatch(
        [#{name := null, open := null}],
        [E || #{endpoint := <<"http://blog.attraktor.org/spaceapi/spaceapi.json">>} = E <- Entries]
    ).

directory_display_fields_test() ->
    Entries = by_name(directory(spacepush_test_util:fixture("aggregator.json"))),
    ?assertMatch(
        #{open := true, address := <<"Schönleinstraße 5, 97080 Würzburg, Germany"/utf8>>, lat := 49.801756},
        maps:get(<<"Nerd2Nerd">>, Entries)
    ),
    ?assertMatch(#{open := false}, maps:get(<<"Hacker Embassy">>, Entries)),
    ?assertMatch(#{open := false}, maps:get(<<"Apollo-NG">>, Entries)),
    ?assertMatch(#{open := null}, maps:get(<<"B4CKSP4CE">>, Entries)),
    ?assertMatch(#{open := null}, maps:get(<<"LuXeria">>, Entries)),
    ?assertMatch(#{open := true}, maps:get(<<"Mainframe">>, Entries)).

directory_accepts_items_object_test() ->
    Body = <<"{\"items\": [{\"url\": \"https://a.example/\", \"lastSeen\": 1791485450,"
             " \"data\": {\"space\": \"A\", \"state\": {\"open\": true}}}]}">>,
    ?assertMatch([#{endpoint := <<"https://a.example/">>, name := <<"A">>, open := true}], directory(Body)).

directory_skips_entries_without_url_test() ->
    ?assertEqual([], directory(<<"[42, {\"url\": 1}, {\"data\": null}]">>)).

entry(Extra) ->
    Entry = maps:merge(
        #{
            <<"url">> => <<"https://a.example/">>,
            <<"lastSeen">> => ?NOW - 60,
            <<"data">> => #{<<"space">> => <<"A">>, <<"state">> => #{<<"open">> => true}}
        },
        Extra
    ),
    [#{open := Open}] = directory(iolist_to_binary(json:encode([Entry]))),
    Open.

fresh_data_counts_test() ->
    ?assertEqual(true, entry(#{})).

stale_data_is_not_shown_test() ->
    ?assertEqual(null, entry(#{<<"lastSeen">> => 1})).

missing_last_seen_is_not_shown_test() ->
    ?assertEqual(null, entry(#{<<"lastSeen">> => null})).

unreachable_endpoint_is_not_shown_test() ->
    ?assertEqual(null, entry(#{<<"validationResult">> => #{<<"reachable">> => false}})).

invalid_schema_still_counts_test() ->
    ?assertEqual(true, entry(#{<<"valid">> => false, <<"validationResult">> => #{<<"reachable">> => true}})).

%% Single SpaceAPI documents

space_test_() ->
    Url = <<"https://a.example/">>,
    [
        ?_assertEqual({{Url, <<"space">>}, <<"A">>, open}, spacepush_state:parse_space(Url, <<"{\"space\":\"A\",\"state\":{\"open\":true}}">>)),
        ?_assertEqual({{Url, <<"space">>}, <<"A">>, closed}, spacepush_state:parse_space(Url, <<"{\"space\":\"A\",\"open\":false}">>)),
        ?_assertEqual({{Url, <<"space">>}, <<"A">>, unknown}, spacepush_state:parse_space(Url, <<"{\"space\":\"A\"}">>)),
        ?_assertError(not_a_space, spacepush_state:parse_space(Url, <<"[1,2]">>)),
        ?_assertError(not_a_space, spacepush_state:parse_space(Url, <<"{\"state\":{\"open\":true}}">>)),
        ?_assertError(_, spacepush_state:parse_space(Url, <<"<html>">>))
    ].

mainframe_document_test() ->
    ?assertEqual(
        {{?MAINFRAME, <<"space">>}, <<"Mainframe">>, open},
        spacepush_state:parse_space(?MAINFRAME, spacepush_test_util:fixture("mainframe-spaceinfo.json"))
    ).

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
