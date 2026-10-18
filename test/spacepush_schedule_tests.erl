-module(spacepush_schedule_tests).
-include_lib("eunit/include/eunit.hrl").

-define(A, <<"https://a.example/">>).
-define(B, <<"https://b.example/">>).

select(Urls, Active, Previous, Now) ->
    spacepush_schedule:select(Urls, Active, Previous, Now, 120000, 600000, 10).

subscribers_poll_every_two_minutes_test() ->
    %% Monotonic time may be negative, including during startup.
    Now = -1000000,
    {[?A], Schedule} = select([?A], [?A], #{}, Now),
    ?assertEqual({[], Schedule}, select([?A], [?A], Schedule, Now + 119999)),
    {[?A], Later} = select([?A], [?A], Schedule, Now + 120000),
    ?assertMatch(#{?A := #{next := -760000}}, Later).

background_start_is_spread_then_repeats_every_ten_minutes_test() ->
    Now = -1000000,
    Urls = [integer_to_binary(N) || N <- lists:seq(1, 100)],
    {_, Schedule} = select(Urls, [], #{}, Now),
    Deadlines = [T || _ := #{next := T} <- Schedule],
    ?assert(lists:all(fun(T) -> T >= Now andalso T =< Now + 600000 end, Deadlines)),
    ?assert(length(lists:usort(Deadlines)) > 90),
    Deadline = maps:get(next, maps:get(?A, element(2, select([?A], [], #{}, Now)))),
    {[?A], S1} = select([?A], [], #{?A => #{next => Deadline, interval => 600000}}, Deadline),
    ?assertEqual({[], S1}, select([?A], [], S1, Deadline + 599999)),
    ?assertMatch({[?A], _}, select([?A], [], S1, Deadline + 600000)).

subscription_changes_adjust_deadlines_test() ->
    {[], S0} = select([?A], [], #{?A => #{next => 500000, interval => 600000}}, 0),
    {[?A], S1} = select([?A], [?A], S0, 1000),
    {[], S2} = select([?A], [], S1, 2000),
    ?assertMatch(#{?A := #{next := 601000, interval := 600000}}, S2),
    ?assertMatch({[?A], _}, select([?A], [], S2, 601000)).

subscribers_have_priority_and_batch_is_bounded_test() ->
    Old = #{?A => #{next => 0, interval => 600000}, ?B => #{next => 0, interval => 120000}},
    {[?B], S1} = spacepush_schedule:select([?A, ?B], [?B], Old, 1, 120000, 600000, 1),
    ?assertMatch({[?A], _}, spacepush_schedule:select([?A, ?B], [?B], S1, 2, 120000, 600000, 1)).

removed_endpoints_drop_deadlines_test() ->
    {_, S0} = select([?A, ?B], [?A, ?B], #{}, 0),
    {[], S1} = select([?A], [?A], S0, 1000),
    ?assertEqual([?A], maps:keys(S1)).

late_rounds_do_not_catch_up_with_a_request_burst_test() ->
    {[?A], S0} = select([?A], [?A], #{}, 0),
    {[?A], S1} = select([?A], [?A], S0, 900000),
    ?assertEqual({[], S1}, select([?A], [?A], S1, 900001)).
