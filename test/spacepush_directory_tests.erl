-module(spacepush_directory_tests).
-include_lib("eunit/include/eunit.hrl").

-define(MAINFRAME, <<"https://status.mainframe.io/api/spaceInfo">>).

directory_test_() ->
    {foreach,
        fun() ->
            spacepush_test_util:setup_env("directory"),
            application:set_env(spacepush, max_data_age_s, 1000000000)
        end,
        fun(_) -> catch gen_server:stop(spacepush_directory) end,
        [
            fun loads_the_aggregator_list/0,
            fun keeps_the_saved_list_when_the_aggregator_fails/0,
            fun starts_empty_without_list/0,
            fun loads_a_list_larger_than_a_space_document/0
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
