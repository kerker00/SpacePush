-module(spacepush_test_util).
-moduledoc "Helpers shared by the test modules.".

-export([tmp_dir/1, fixture/1, setup_env/1, enqueue_in_fresh_node/1]).

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
    Dir.

-doc "Runs in a peer node: starts registry and outbox in `Dir` and enqueues one delivery.".
enqueue_in_fresh_node(Dir) ->
    _ = application:load(spacepush),
    application:set_env(spacepush, registry_file, filename:join(Dir, "registry.dets")),
    application:set_env(spacepush, outbox_file, filename:join(Dir, "outbox.dets")),
    [begin {ok, Pid} = M:start_link(), unlink(Pid) end || M <- [spacepush_registry, spacepush_outbox]],
    Topic = {<<"https://a.example/">>, <<"space">>},
    ok = spacepush_registry:register(binary:copy(<<"ab">>, 32), sandbox, [Topic]),
    ok = spacepush_outbox:enqueue([{Topic, <<"A">>, closed, open}]).
