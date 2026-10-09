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
            fun starts_empty_without_list/0
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
