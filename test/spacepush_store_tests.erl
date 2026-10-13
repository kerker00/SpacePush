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

directory_read_error_is_explicit_test() ->
    File = spacepush_test_util:tmp_dir("store-directory"),
    ?assertError({state_file_read_failed, File, eisdir}, spacepush_store:load(File, default)).

permission_error_is_explicit_test() ->
    File = file("private.bin"),
    ok = spacepush_store:save(File, #{saved => true}),
    ok = file:change_mode(File, 0),
    try ?assertError({state_file_read_failed, File, eacces}, spacepush_store:load(File, default))
    after ok = file:change_mode(File, 8#600) end.

trailing_bytes_are_not_accepted_test() ->
    File = file("trailing.bin"),
    Binary = term_to_binary(#{saved => true}),
    ok = file:write_file(File, <<Binary/binary, 0>>),
    ?assertEqual(default, spacepush_store:load(File, default)),
    ?assertError({invalid_state_file, File}, spacepush_store:read(File)).

with_limit(Limit, Fun) ->
    Previous = application:get_env(spacepush, state_max_bytes, 67108864),
    application:set_env(spacepush, state_max_bytes, Limit),
    try Fun() after application:set_env(spacepush, state_max_bytes, Previous) end.

oversized_read_fails_closed_test() ->
    File = file("large.bin"),
    ok = file:write_file(File, term_to_binary(binary:copy(<<0>>, 256))),
    with_limit(128, fun() ->
        ?assertError({state_file_too_large, File, 128}, spacepush_store:load(File, default))
    end).

compressed_size_is_checked_before_decode_test() ->
    File = file("compressed.bin"),
    Binary = term_to_binary(binary:copy(<<0>>, 1024), [compressed]),
    ?assert(byte_size(Binary) < 128),
    ok = file:write_file(File, Binary),
    with_limit(128, fun() ->
        ?assertError({state_file_too_large, File, 128}, spacepush_store:read(File))
    end).

oversized_save_preserves_previous_file_test() ->
    File = file("previous.bin"),
    ok = spacepush_store:save(File, #{saved => true}),
    with_limit(128, fun() ->
        ?assertError({state_file_too_large, File, 128}, spacepush_store:save(File, binary:copy(<<0>>, 256))),
        ?assertEqual({ok, #{saved => true}}, spacepush_store:read(File)),
        ?assertNot(filelib:is_file(File ++ ".tmp"))
    end).

rename_error_is_explicit_test() ->
    File = spacepush_test_util:tmp_dir("store-target-directory"),
    ?assertError({state_file_write_failed, File, rename, eisdir}, spacepush_store:save(File, #{})),
    ok = file:delete(File ++ ".tmp").
