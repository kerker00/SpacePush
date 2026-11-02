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
        fun concurrent_client_admission_respects_limit/0,
        fun concurrent_total_admission_respects_limit/0,
        fun total_limit_is_released_after_a_crash/0,
        fun rejected_admission_leaves_no_monitor/0,
        fun restart_during_admission_returns_unavailable/0,
        fun test_event_counts_recipients/0
    ]}.

setup() ->
    _ = application:load(spacepush),
    application:set_env(spacepush, stream_max_per_client, 2),
    application:set_env(spacepush, stream_max_connections, 10000),
    {ok, Pid} = spacepush_stream:start_link(),
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

concurrent_client_admission_respects_limit() ->
    application:set_env(spacepush, stream_max_per_client, 1),
    Results = concurrent_subscriptions(lists:duplicate(24, client_a)),
    ?assertEqual(1, length([ok || {ok, _} <- Results])),
    ?assertEqual(23, length([error || {error, too_many_streams} <- Results])).

concurrent_total_admission_respects_limit() ->
    application:set_env(spacepush, stream_max_connections, 3),
    Results = concurrent_subscriptions(lists:seq(1, 24)),
    ?assertEqual(3, length([ok || {ok, _} <- Results])),
    ?assertEqual(21, length([error || {error, full} <- Results])).

total_limit_is_released_after_a_crash() ->
    application:set_env(spacepush, stream_max_connections, 1),
    First = subscriber([?TOPIC_A], client_a),
    ?assertEqual({error, full}, subscribe([?TOPIC_B], client_b)),
    exit(First, kill),
    wait_until(fun() ->
        #{subscriptions := Subscriptions} = sys:get_state(spacepush_stream),
        map_size(Subscriptions) =:= 0
    end),
    Live = subscriber([?TOPIC_B], client_b),
    ?assertEqual(0, spacepush_stream:broadcast_test(?TOPIC_A, <<"A">>)),
    ?assertEqual(1, spacepush_stream:broadcast_test(?TOPIC_B, <<"B">>)),
    ?assertMatch({<<"test">>, _}, receive_event(Live)).

rejected_admission_leaves_no_monitor() ->
    application:set_env(spacepush, stream_max_connections, 0),
    Before = process_info(self(), monitors),
    ?assertEqual({error, full}, spacepush_stream:subscribe([?TOPIC_A], client_a)),
    ?assertEqual(Before, process_info(self(), monitors)).

restart_during_admission_returns_unavailable() ->
    Server = whereis(spacepush_stream),
    ok = sys:suspend(Server),
    Parent = self(),
    spawn(fun() -> Parent ! {admission_result, spacepush_stream:subscribe([?TOPIC_A], client_a)} end),
    wait_until(fun() ->
        {message_queue_len, N} = process_info(Server, message_queue_len), N >= 1
    end),
    exit(Server, kill),
    wait_down(Server),
    {ok, Replacement} = spacepush_stream:start_link(),
    unlink(Replacement),
    try
        receive
            {admission_result, Result} -> ?assertEqual({error, unavailable}, Result)
        after 2000 -> error(admission_timeout)
        end,
        ?assertMatch(#{subscriptions := Subs} when map_size(Subs) =:= 0,
            sys:get_state(Replacement))
    after gen_server:stop(Replacement) end.

concurrent_subscriptions(Clients) ->
    Parent = self(),
    Server = whereis(spacepush_stream),
    ok = sys:suspend(Server),
    Pids = [spawn(fun() ->
        Result = spacepush_stream:subscribe([?TOPIC_A], Client),
        Parent ! {joined, self(), Result},
        case Result of
            {ok, Ref} -> receive stop -> ok; {'DOWN', Ref, _, _, _} -> ok end;
            _ -> ok
        end
    end) || Client <- Clients],
    try
        %% Every call waits for the same server, forcing concurrent admission.
        wait_until(fun() ->
            {message_queue_len, N} = process_info(Server, message_queue_len),
            N >= length(Pids)
        end)
    after ok = sys:resume(Server) end,
    [receive {joined, Pid, Result} -> Result after 2000 -> error(admission_timeout) end || Pid <- Pids].

test_event_counts_recipients() ->
    S = subscriber([?TOPIC_A], client_a),
    ?assertEqual(1, spacepush_stream:broadcast_test(?TOPIC_A, <<"A">>)),
    ?assertMatch({<<"test">>, #{<<"test">> := true, <<"endpoint">> := ?A}}, receive_event(S)),
    ?assertEqual(0, spacepush_stream:broadcast_test(?TOPIC_B, <<"B">>)).

missing_scope_test() ->
    %% Without the scope nothing is delivered, but the caller must not fail.
    ?assertEqual(undefined, whereis(spacepush_stream)),
    ?assertEqual({error, unavailable}, spacepush_stream:subscribe([?TOPIC_A], client_a)),
    ?assertEqual(ok, spacepush_stream:broadcast([{?TOPIC_A, <<"A">>, closed, open}])),
    ?assertEqual(0, spacepush_stream:broadcast_test(?TOPIC_A, <<"A">>)).

event_format_test() ->
    Event = spacepush_stream:event(change, #{<<"name">> => <<"a\nb">>}),
    ?assertEqual(<<"event: change\ndata: {\"name\":\"a\\nb\"}\n\n">>, Event).

%% A process that subscribes, then forwards stream messages to the test.
subscriber(Topics, Client) ->
    Test = self(),
    Pid = spawn(fun() ->
        {ok, Monitor} = spacepush_stream:subscribe(Topics, Client),
        Test ! {joined, self(), ok},
        forward(Test, Monitor)
    end),
    receive {joined, Pid, ok} -> Pid end.

forward(Test, Monitor) ->
    receive
        stop -> ok;
        {'DOWN', Monitor, _, _, _} -> ok;
        {spacepush_stream, Event} -> Test ! {event, self(), Event}, forward(Test, Monitor)
    end.

subscribe(Topics, Client) ->
    Test = self(),
    Pid = spawn(fun() ->
        case spacepush_stream:subscribe(Topics, Client) of
            {ok, Monitor} ->
                Test ! {joined, self(), ok},
                receive stop -> ok; {'DOWN', Monitor, _, _, _} -> ok end;
            Error -> Test ! {joined, self(), Error}
        end
    end),
    receive {joined, Pid, Result} -> Result end.

stop(Pid) ->
    Pid ! stop,
    wait_down(Pid),
    wait_until(fun() ->
        #{subscriptions := Subscriptions} = sys:get_state(spacepush_stream),
        not maps:is_key(Pid, Subscriptions)
    end).

wait_until(Predicate) ->
    wait_until(Predicate, erlang:monotonic_time(millisecond) + 2000).

wait_until(Predicate, Deadline) ->
    case Predicate() of
        true -> ok;
        false ->
            ?assert(erlang:monotonic_time(millisecond) < Deadline),
            receive after 1 -> ok end,
            wait_until(Predicate, Deadline)
    end.

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
