-module(spacepush_store_tests).
-include_lib("eunit/include/eunit.hrl").

file(Name) -> filename:join(spacepush_test_util:tmp_dir("store"), Name).

roundtrip_test() ->
    File = file("state.bin"),
    ok = spacepush_store:save(File, #{a => 1}),
    ?assertEqual(#{a => 1}, spacepush_store:load(File, default)),
    ?assertNot(filelib:is_file(File ++ ".tmp")).

missing_file_test() ->
    ?assertEqual(default, spacepush_store:load(file("missing.bin"), default)).

corrupt_file_test() ->
    File = file("corrupt.bin"),
    ok = file:write_file(File, <<"not a term">>),
    ?assertEqual(default, spacepush_store:load(File, default)).
