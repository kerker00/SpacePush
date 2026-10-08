-module(spacepush_test_util).
-moduledoc "Helpers shared by the test modules.".

-export([tmp_dir/1, fixture/1, setup_env/1]).

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
