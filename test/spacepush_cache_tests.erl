-module(spacepush_cache_tests).
-include_lib("eunit/include/eunit.hrl").

-define(URL, <<"https://a.example/">>).
-define(SPACE, <<"{\"space\":\"A\",\"state\":{\"open\":true}}">>).

cache_test_() ->
    {foreach,
        fun() ->
            spacepush_test_util:setup_env("cache"),
            application:set_env(spacepush, cache_max_age_ms, 60000),
            application:set_env(spacepush, on_demand_fetch_limit, 20),
            {ok, Pid} = spacepush_cache:start_link(),
            unlink(Pid)
        end,
        fun(_) -> gen_server:stop(spacepush_cache) end,
        [
            fun fetches_once_then_serves_cached/0,
            {timeout, 10, fun concurrent_requests_share_one_fetch/0},
            fun serves_stale_entry_when_fetch_fails/0,
            fun rejects_unusable_response/0,
            fun marks_requests_as_watched/0,
            {timeout, 10, fun limits_parallel_fetches/0}
        ]}.

fetches_once_then_serves_cached() ->
    spacepush_test_util:fake_responses(#{?URL => {ok, ?SPACE}}),
    ?assertMatch({ok, #{body := ?SPACE, observations := [{_, <<"A">>, open}]}}, spacepush_cache:get(?URL)),
    ?assertMatch({ok, #{body := ?SPACE}}, spacepush_cache:get(?URL)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)).

concurrent_requests_share_one_fetch() ->
    spacepush_test_util:fake_responses(#{?URL => {delay, 200, {ok, ?SPACE}}}),
    Self = self(),
    [spawn(fun() -> Self ! {done, spacepush_cache:get(?URL)} end) || _ <- lists:seq(1, 5)],
    Results = [receive {done, R} -> R after 5000 -> timeout end || _ <- lists:seq(1, 5)],
    ?assert(lists:all(fun(R) -> element(1, R) =:= ok end, Results)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)).

serves_stale_entry_when_fetch_fails() ->
    spacepush_cache:store(?URL, ?SPACE, []),
    application:set_env(spacepush, cache_max_age_ms, 0),
    spacepush_test_util:fake_responses(#{?URL => {error, timeout}}),
    ?assertMatch({ok, #{body := ?SPACE}}, spacepush_cache:get(?URL)),
    ?assertEqual(1, spacepush_test_util:fake_calls(x)).

rejects_unusable_response() ->
    spacepush_test_util:fake_responses(#{?URL => {ok, <<"<html>">>}}),
    ?assertEqual({error, invalid_response}, spacepush_cache:get(?URL)),
    ?assertEqual(none, spacepush_cache:lookup(?URL)).

marks_requests_as_watched() ->
    Before = erlang:system_time(millisecond),
    spacepush_test_util:fake_responses(#{?URL => {ok, ?SPACE}}),
    {ok, _} = spacepush_cache:get(?URL),
    ?assertEqual([?URL], spacepush_cache:watched(Before)),
    ?assertEqual([], spacepush_cache:watched(erlang:system_time(millisecond) + 1)).

limits_parallel_fetches() ->
    application:set_env(spacepush, on_demand_fetch_limit, 1),
    Other = <<"https://b.example/">>,
    spacepush_test_util:fake_responses(#{?URL => {delay, 300, {ok, ?SPACE}}, Other => {ok, ?SPACE}}),
    Self = self(),
    spawn(fun() -> Self ! {slow, spacepush_cache:get(?URL)} end),
    timer:sleep(50),
    ?assertEqual({error, busy}, spacepush_cache:get(Other)),
    ?assertMatch({slow, {ok, _}}, receive Msg -> Msg after 5000 -> timeout end).
