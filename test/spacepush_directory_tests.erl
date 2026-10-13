-module(spacepush_directory_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MAINFRAME, <<"https://status.mainframe.io/api/spaceInfo">>).

directory_test_() ->
    {foreach,
        fun() ->
            spacepush_test_util:setup_env("directory"),
            application:set_env(spacepush, max_data_age_s, 1000000000),
            {ok, Pid} = spacepush_cache:start_link(),
            unlink(Pid)
        end,
        fun(_) ->
            try gen_server:stop(spacepush_directory) catch _:_ -> ok end,
            gen_server:stop(spacepush_cache)
        end,
        [
            fun loads_the_aggregator_list/0,
            fun keeps_the_saved_list_when_the_aggregator_fails/0,
            fun starts_empty_without_list/0,
            fun loads_a_list_larger_than_a_space_document/0,
            fun restored_old_list_keeps_endpoints_without_open_flags/0,
            fun restored_partial_entries_are_safe_to_serve/0,
            fun invalid_optional_metadata_preserves_the_endpoint/0,
            fun unsupported_saved_format_starts_empty/0
        ]}.

aggregator() ->
    {ok, Url} = application:get_env(spacepush, aggregator_url),
    list_to_binary(Url).

start() ->
    {ok, Pid} = spacepush_directory:start_link(),
    unlink(Pid),
    %% The first refresh runs as a message after init; a call waits for it.
    sys:get_state(spacepush_directory).

loads_the_aggregator_list() ->
    spacepush_test_util:fake_responses(#{aggregator() => {ok, spacepush_test_util:fixture("aggregator.json")}}),
    start(),
    ?assert(spacepush_directory:loaded()),
    ?assert(spacepush_directory:known(?MAINFRAME)),
    ?assert(spacepush_directory:known(<<"http://blog.attraktor.org/spaceapi/spaceapi.json">>)),
    ?assertNot(spacepush_directory:known(<<"http://127.0.0.1:8080/">>)),
    ?assertEqual(8, length(spacepush_directory:entries())).

keeps_the_saved_list_when_the_aggregator_fails() ->
    spacepush_test_util:fake_responses(#{aggregator() => {ok, spacepush_test_util:fixture("aggregator.json")}}),
    start(),
    gen_server:stop(spacepush_directory),
    spacepush_test_util:fake_responses(#{aggregator() => {error, timeout}}),
    start(),
    ?assert(spacepush_directory:known(?MAINFRAME)),
    ?assertEqual(8, length(spacepush_directory:entries())).

starts_empty_without_list() ->
    spacepush_test_util:fake_responses(#{aggregator() => {error, timeout}}),
    start(),
    ?assertNot(spacepush_directory:loaded()),
    ?assertNot(spacepush_directory:known(?MAINFRAME)).

%% The real aggregator list is far above the 256 KB allowed for a single space.
loads_a_list_larger_than_a_space_document() ->
    {ok, [Entry | _]} = {ok, json:decode(spacepush_test_util:fixture("aggregator.json"))},
    Entries = [Entry#{<<"url">> => <<"https://s", (integer_to_binary(N))/binary, ".example/">>} || N <- lists:seq(1, 600)],
    Body = iolist_to_binary(json:encode(Entries)),
    ?assert(byte_size(Body) > 262144),
    spacepush_test_util:fake_responses(#{aggregator() => {ok, Body}}),
    start(),
    ?assertEqual(600, length(spacepush_directory:entries())).

restored_old_list_keeps_endpoints_without_open_flags() ->
    application:set_env(spacepush, max_data_age_s, 900),
    Seen = erlang:system_time(second) - 901,
    Raw = #{<<"url">> => <<"https://a.example/">>, <<"lastSeen">> => Seen,
        <<"data">> => #{<<"space">> => <<"A">>, <<"state">> => #{<<"open">> => true}}},
    Entries = spacepush_state:parse_directory(iolist_to_binary(json:encode([Raw])), Seen, 900),
    ?assertMatch([#{open := true}], Entries),
    {ok, File} = application:get_env(spacepush, directory_file),
    ok = spacepush_store:save(File, {directory, 1, Entries}),
    spacepush_test_util:fake_responses(#{aggregator() => {error, timeout}}),
    start(),
    ?assert(spacepush_directory:known(<<"https://a.example/">>)),
    Directory = spacepush_http_read:directory(erlang:system_time(millisecond)),
    ?assertMatch([#{<<"data">> := #{<<"space">> := <<"A">>}}], Directory),
    ?assertEqual(#{<<"open">> => 0, <<"total">> => 1, <<"as_of">> => null}, spacepush_http_read:summary(Directory)).

saved_list(Entries) ->
    {ok, File} = application:get_env(spacepush, directory_file),
    ok = spacepush_store:save(File, {directory, 1, Entries}),
    spacepush_test_util:fake_responses(#{aggregator() => {error, timeout}}),
    start().

restored_partial_entries_are_safe_to_serve() ->
    Endpoint = <<"https://a.example/">>,
    saved_list([#{endpoint => Endpoint}, garbage, #{endpoint => 42}, #{endpoint => <<255>>}]),
    ?assert(spacepush_directory:known(Endpoint)),
    ?assertEqual(1, length(spacepush_directory:entries())),
    [Entry] = spacepush_directory:entries(),
    ?assertMatch(#{name := null, address := null, lat := null, lon := null, open := null, last_seen := null}, Entry),
    Directory = spacepush_http_read:directory(erlang:system_time(millisecond)),
    ?assert(is_binary(iolist_to_binary(json:encode(Directory)))),
    ?assertEqual(#{<<"open">> => 0, <<"total">> => 0, <<"as_of">> => null}, spacepush_http_read:summary(Directory)).

invalid_optional_metadata_preserves_the_endpoint() ->
    Endpoint = <<"https://a.example/">>,
    saved_list([#{endpoint => Endpoint, name => <<255>>, address => [], lat => 91, lon => -181,
        open => yes, last_seen => 1 bsl 128, unwanted => <<"discard">>}]),
    ?assertEqual([#{endpoint => Endpoint, name => null, address => null, lat => null, lon => null,
        open => null, last_seen => null}], spacepush_directory:entries()),
    ?assert(spacepush_directory:known(Endpoint)).

unsupported_saved_format_starts_empty() ->
    {ok, File} = application:get_env(spacepush, directory_file),
    ok = spacepush_store:save(File, {directory, 99, [#{endpoint => <<"https://a.example/">>}]}),
    start(),
    ?assertNot(spacepush_directory:loaded()).
