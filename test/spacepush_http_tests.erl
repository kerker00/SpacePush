-module(spacepush_http_tests).
-include_lib("eunit/include/eunit.hrl").

req(Forwarded) ->
    Headers =
        case Forwarded of
            undefined -> #{};
            _ -> #{<<"x-forwarded-for">> => Forwarded}
        end,
    #{headers => Headers, peer => {{10, 0, 0, 1}, 4711}}.

client_test_() ->
    {foreach, fun() -> application:load(spacepush) end, fun(_) -> application:set_env(spacepush, trust_proxy, false) end, [
        fun() ->
            application:set_env(spacepush, trust_proxy, false),
            ?assertEqual({10, 0, 0, 1}, spacepush_http:client(req(<<"1.2.3.4">>)))
        end,
        fun() ->
            application:set_env(spacepush, trust_proxy, true),
            %% The proxy appends what it saw; anything before that is the client's claim.
            ?assertEqual(<<"203.0.113.7">>, spacepush_http:client(req(<<"6.6.6.6, 203.0.113.7">>))),
            ?assertEqual(<<"203.0.113.7">>, spacepush_http:client(req(<<"203.0.113.7">>))),
            ?assertEqual({10, 0, 0, 1}, spacepush_http:client(req(undefined)))
        end
    ]}.

summary_test_() ->
    Entry = fun(Open, Seen) -> #{<<"url">> => <<"https://a.example/">>, <<"lastSeen">> => Seen, <<"data">> => #{<<"state">> => #{<<"open">> => Open}}} end,
    Unknown = #{<<"url">> => <<"https://b.example/">>, <<"data">> => #{<<"space">> => <<"B">>}},
    [
        ?_assertEqual(
            #{<<"open">> => 2, <<"total">> => 4, <<"as_of">> => <<"2026-10-10T10:00:00Z">>},
            spacepush_http_read:summary([Entry(true, 1791626400), Entry(false, 1791626000), Entry(true, 1791620000), Unknown])
        ),
        ?_assertEqual(#{<<"open">> => 0, <<"total">> => 0, <<"as_of">> => null}, spacepush_http_read:summary([]))
    ].

summary_ignores_timestamps_without_known_states_test() ->
    Unknown = #{<<"url">> => <<"https://a.example/">>, <<"lastSeen">> => 1 bsl 200, <<"data">> => #{<<"space">> => <<"A">>}},
    ?assertEqual(#{<<"open">> => 0, <<"total">> => 1, <<"as_of">> => null}, spacepush_http_read:summary([Unknown])).

summary_accepts_fractional_observation_times_test() ->
    Entry = #{<<"url">> => <<"https://a.example/">>, <<"lastSeen">> => 1791626400.5,
        <<"data">> => #{<<"state">> => #{<<"open">> => true}}},
    ?assertEqual(#{<<"open">> => 1, <<"total">> => 1, <<"as_of">> => <<"2026-10-10T10:00:00Z">>}, spacepush_http_read:summary([Entry])).

-define(A, <<"https://a.example/">>).

directory_freshness_test_() ->
    {foreach, fun() ->
        spacepush_test_util:setup_env("read_freshness"),
        {ok, PreviousAge} = application:get_env(spacepush, max_data_age_s),
        application:set_env(spacepush, max_data_age_s, 900),
        ets:new(spacepush_directory, [named_table, public, set]),
        {ok, Pid} = spacepush_cache:start_link(),
        unlink(Pid),
        PreviousAge
    end, fun(PreviousAge) ->
        gen_server:stop(spacepush_cache),
        ets:delete(spacepush_directory),
        application:set_env(spacepush, max_data_age_s, PreviousAge)
    end, [
        fun directory_flags_expire_without_refresh/0,
        fun recent_cache_overrides_expired_aggregator/0,
        fun expired_cache_and_aggregator_are_unknown/0,
        fun future_aggregator_timestamp_is_unknown/0,
        fun future_cache_does_not_override_current_directory/0
    ]}.

put_directory(LastSeen, Open) ->
    ets:insert(spacepush_directory, {?A, #{endpoint => ?A, name => <<"A">>, address => null, lat => null, lon => null, open => Open, last_seen => LastSeen}}).

put_cache(FetchedAt, State) ->
    Open = case State of open -> true; closed -> false; _ -> null end,
    Body = iolist_to_binary(json:encode(#{<<"space">> => <<"A">>, <<"state">> => #{<<"open">> => Open}})),
    spacepush_cache:store(?A, Body, [{{?A, <<"space">>}, <<"A">>, State}]),
    Entry = spacepush_cache:lookup(?A),
    ets:insert(spacepush_cache, {?A, Entry#{fetched_at := FetchedAt}}).

directory_flags_expire_without_refresh() ->
    put_directory(990, true),
    ?assertMatch([#{<<"data">> := #{<<"state">> := #{<<"open">> := true}}}], spacepush_http_read:directory(1000000)),
    ?assertMatch([#{<<"data">> := #{<<"state">> := #{<<"open">> := true}}}], spacepush_http_read:directory(1890000)),
    [Expired] = spacepush_http_read:directory(1891000),
    ?assertEqual(#{<<"space">> => <<"A">>}, maps:get(<<"data">>, Expired)),
    ?assertEqual(990, maps:get(<<"lastSeen">>, Expired)),
    ?assert(spacepush_directory:known(?A)),
    ?assertEqual(#{<<"open">> => 0, <<"total">> => 1, <<"as_of">> => null}, spacepush_http_read:summary([Expired])).

recent_cache_overrides_expired_aggregator() ->
    put_directory(990, false),
    put_cache(1900000, open),
    Directory = spacepush_http_read:directory(1901000),
    ?assertMatch([#{<<"lastSeen">> := 1900, <<"data">> := #{<<"state">> := #{<<"open">> := true}}}], Directory),
    ?assertMatch(#{<<"open">> := 1, <<"total">> := 1}, spacepush_http_read:summary(Directory)).

expired_cache_and_aggregator_are_unknown() ->
    put_directory(990, true),
    put_cache(1000000, open),
    Directory = spacepush_http_read:directory(1901000),
    ?assertMatch([#{<<"data">> := #{<<"space">> := <<"A">>}}], Directory),
    ?assertEqual(0, maps:get(<<"open">>, spacepush_http_read:summary(Directory))),
    [Entry] = Directory,
    ?assertNot(maps:is_key(<<"state">>, maps:get(<<"data">>, Entry))).

future_aggregator_timestamp_is_unknown() ->
    put_directory(2000, true),
    [Entry] = spacepush_http_read:directory(1901000),
    ?assertNot(maps:is_key(<<"state">>, maps:get(<<"data">>, Entry))),
    ?assertEqual(null, maps:get(<<"as_of">>, spacepush_http_read:summary([Entry]))).

future_cache_does_not_override_current_directory() ->
    put_directory(1900, false),
    put_cache(1910000, open),
    ?assertMatch([#{<<"lastSeen">> := 1900, <<"data">> := #{<<"state">> := #{<<"open">> := false}}}], spacepush_http_read:directory(1901000)).
