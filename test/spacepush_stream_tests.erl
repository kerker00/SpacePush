-module(spacepush_stream_tests).
-include_lib("eunit/include/eunit.hrl").

-define(A, <<"https://a.example/">>).
-define(B, <<"https://b.example/">>).
-define(TOPIC_A, {?A, <<"space">>}).
-define(TOPIC_B, {?B, <<"space">>}).

stream_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun broadcast_reaches_only_matching_topic/0,
        fun room_and_cleaned_name_are_sent/0,
        fun broadcast_without_subscribers_is_harmless/0,
        fun ended_subscriber_is_ignored/0,
        fun client_limit_is_released_when_a_stream_ends/0,
        fun test_event_counts_recipients/0
    ]}.

setup() ->
    _ = application:load(spacepush),
    application:set_env(spacepush, stream_max_per_client, 2),
    {ok, Pid} = pg:start_link(spacepush_stream),
    unlink(Pid),
    Pid.

cleanup(Pid) ->
    exit(Pid, kill),
    wait_down(Pid).

broadcast_reaches_only_matching_topic() ->
    A = subscriber([?TOPIC_A], client_a),
    B = subscriber([?TOPIC_B], client_b),
    ok = spacepush_stream:broadcast([{?TOPIC_A, <<"A">>, closed, open}]),
    {Kind, Data} = receive_event(A),
    ?assertEqual(<<"change">>, Kind),
    ?assertMatch(#{<<"endpoint">> := ?A, <<"room">> := <<"space">>, <<"name">> := <<"A">>,
        <<"from">> := <<"closed">>, <<"to">> := <<"open">>, <<"at">> := At} when is_integer(At), Data),
    ?assertEqual(timeout, receive_event(B, 100)).

room_and_cleaned_name_are_sent() ->
    Topic = {spacepush_state:mainframe_endpoint(), <<"radstelle">>},
    S = subscriber([Topic], client_a),
    ok = spacepush_stream:broadcast([{Topic, <<"Main\nframe">>, open, closed}]),
    {_Kind, Data} = receive_event(S),
    ?assertEqual(<<"Mainframe · Radstelle"/utf8>>, maps:get(<<"name">>, Data)).

broadcast_without_subscribers_is_harmless() ->
    ?assertEqual(ok, spacepush_stream:broadcast([{?TOPIC_A, <<"A">>, closed, open}])),
    ?assertEqual(ok, spacepush_stream:broadcast([])).

ended_subscriber_is_ignored() ->
    S = subscriber([?TOPIC_A], client_a),
    stop(S),
    Live = subscriber([?TOPIC_A], client_a),
    ok = spacepush_stream:broadcast([{?TOPIC_A, <<"A">>, closed, open}]),
    ?assertMatch({<<"change">>, _}, receive_event(Live)).

client_limit_is_released_when_a_stream_ends() ->
    First = subscriber([?TOPIC_A], client_a),
    _Second = subscriber([?TOPIC_A], client_a),
    ?assertEqual({error, too_many_streams}, subscribe([?TOPIC_A], client_a)),
    ?assertEqual(ok, subscribe([?TOPIC_A], client_b)),
    stop(First),
    ?assertEqual(ok, subscribe([?TOPIC_A], client_a)).

test_event_counts_recipients() ->
    S = subscriber([?TOPIC_A], client_a),
    ?assertEqual(1, spacepush_stream:broadcast_test(?TOPIC_A, <<"A">>)),
    ?assertMatch({<<"test">>, #{<<"test">> := true, <<"endpoint">> := ?A}}, receive_event(S)),
    ?assertEqual(0, spacepush_stream:broadcast_test(?TOPIC_B, <<"B">>)).

missing_scope_test() ->
    %% Without the scope nothing is delivered, but the caller must not fail.
    ?assertEqual(undefined, whereis(spacepush_stream)),
    ?assertEqual(ok, spacepush_stream:broadcast([{?TOPIC_A, <<"A">>, closed, open}])),
    ?assertEqual(0, spacepush_stream:broadcast_test(?TOPIC_A, <<"A">>)).

event_format_test() ->
    Event = spacepush_stream:event(change, #{<<"name">> => <<"a\nb">>}),
    ?assertEqual(<<"event: change\ndata: {\"name\":\"a\\nb\"}\n\n">>, Event).

%% A process that joins the groups, then forwards stream messages to the test.
subscriber(Topics, Client) ->
    Test = self(),
    Pid = spawn(fun() ->
        Test ! {joined, self(), spacepush_stream:subscribe(Topics, Client)},
        forward(Test)
    end),
    receive {joined, Pid, ok} -> Pid end.

forward(Test) ->
    receive
        stop -> ok;
        {spacepush_stream, Event} -> Test ! {event, self(), Event}, forward(Test)
    end.

subscribe(Topics, Client) ->
    Test = self(),
    Pid = spawn(fun() -> Test ! {joined, self(), spacepush_stream:subscribe(Topics, Client)}, receive stop -> ok end end),
    receive {joined, Pid, Result} -> Result end.

stop(Pid) ->
    Pid ! stop,
    wait_down(Pid),
    %% pg removes members asynchronously on their exit.
    sys:get_state(spacepush_stream).

wait_down(Pid) ->
    Ref = monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _} -> ok end.

receive_event(Pid) ->
    receive_event(Pid, 1000).

receive_event(Pid, Timeout) ->
    receive
        {event, Pid, Event} ->
            [<<"event: ", Kind/binary>>, <<"data: ", Json/binary>>, <<>>, <<>>] = binary:split(Event, <<"\n">>, [global]),
            {Kind, json:decode(Json)}
    after Timeout -> timeout
    end.
