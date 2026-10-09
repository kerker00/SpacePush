-module(spacepush_test_util).
-moduledoc "Helpers shared by the test modules.".

-export([tmp_dir/1, fixture/1, setup_env/1, enqueue_in_fresh_node/1]).
-export([fake_responses/1, fake_get/1, fake_calls/1]).

-doc "An empty directory inside `_build`, so tests never write outside the project.".
tmp_dir(Name) ->
    Dir = filename:join([filename:dirname(?FILE), "..", "_build", "test", "tmp", Name]),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_path(Dir),
    Dir.

fixture(Name) ->
    {ok, Body} = file:read_file(filename:join([filename:dirname(?FILE), "fixtures", Name])),
    Body.

-doc "Loads the application defaults and points all files into a fresh directory.".
setup_env(Name) ->
    _ = application:load(spacepush),
    Dir = tmp_dir(Name),
    application:set_env(spacepush, registry_file, filename:join(Dir, "registry.dets")),
    application:set_env(spacepush, outbox_file, filename:join(Dir, "outbox.dets")),
    application:set_env(spacepush, tracker_file, filename:join(Dir, "tracker.bin")),
    application:set_env(spacepush, directory_file, filename:join(Dir, "directory.bin")),
    application:set_env(spacepush, http_get, {?MODULE, fake_get}),
    fake_responses(#{}),
    Dir.

-doc """
Sets what `fake_get/1` answers per URL: `{ok, Body}`, `{error, Reason}`, or
`{delay, Ms, Result}` to answer after a pause. Unknown URLs give `{error, not_found}`.
""".
fake_responses(Responses) ->
    persistent_term:put({?MODULE, responses}, Responses),
    persistent_term:put({?MODULE, calls}, counters:new(1, [])).

fake_get(Url) ->
    counters:add(persistent_term:get({?MODULE, calls}), 1, 1),
    case maps:get(list_to_binary(Url), persistent_term:get({?MODULE, responses}), {error, not_found}) of
        {delay, Ms, Result} ->
            timer:sleep(Ms),
            Result;
        Result ->
            Result
    end.

-doc "How many requests `fake_get/1` answered since the last `fake_responses/1`.".
fake_calls(_) ->
    counters:get(persistent_term:get({?MODULE, calls}), 1).

-doc "Runs in a peer node: starts registry and outbox in `Dir` and enqueues one delivery.".
enqueue_in_fresh_node(Dir) ->
    _ = application:load(spacepush),
    application:set_env(spacepush, registry_file, filename:join(Dir, "registry.dets")),
    application:set_env(spacepush, outbox_file, filename:join(Dir, "outbox.dets")),
    [begin {ok, Pid} = M:start_link(), unlink(Pid) end || M <- [spacepush_registry, spacepush_outbox]],
    Topic = {<<"https://a.example/">>, <<"space">>},
    ok = spacepush_registry:register(binary:copy(<<"ab">>, 32), sandbox, [Topic]),
    ok = spacepush_outbox:enqueue([{Topic, <<"A">>, closed, open}]).
