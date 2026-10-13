-module(spacepush_store).
-moduledoc "Saves a term by syncing a temporary file and atomically renaming it, and reads it back.".

-include_lib("kernel/include/logger.hrl").

-export([load/2, read/1, save/2]).

-doc """
Returns the saved term, or `Default` if the file is missing or has invalid ETF.
Other read errors and oversized files fail explicitly. Reads and writes are
bounded by `state_max_bytes`, including the declared size of compressed ETF.

Decoding is `safe`, so it never creates atoms: the caller must load the module
that defines the atoms in the term before calling this.
""".
-spec load(file:filename(), term()) -> term().
load(File, Default) ->
    try read(File) of
        {ok, Term} -> Term;
        missing -> Default
    catch
        error:{invalid_state_file, File} ->
            ?LOG_ERROR(#{msg => unreadable_state_file, file => File}),
            Default
    end.

-doc "Strict bounded read; present invalid files raise instead of becoming a default.".
-spec read(file:filename()) -> missing | {ok, term()}.
read(File) ->
    Limit = byte_limit(),
    case file:open(File, [read, raw, binary]) of
        {error, enoent} -> missing;
        {error, Reason} -> error({state_file_read_failed, File, Reason});
        {ok, Fd} ->
            try file:read(Fd, Limit + 1) of
                {ok, Binary} ->
                    check_size(File, byte_size(Binary), Limit),
                    case Binary of
                        <<131, 80, Expanded:32, _/binary>> -> check_size(File, Expanded + 1, Limit);
                        _ -> ok
                    end,
                    try binary_to_term(Binary, [safe, used]) of
                        {Term, Used} when Used =:= byte_size(Binary) -> {ok, Term};
                        _ -> error({invalid_state_file, File})
                    catch error:badarg -> error({invalid_state_file, File}) end;
                eof -> error({invalid_state_file, File});
                {error, ReadReason} -> error({state_file_read_failed, File, ReadReason})
            after ok = file:close(Fd) end
    end.

byte_limit() ->
    Limit = application:get_env(spacepush, state_max_bytes, 64 * 1024 * 1024),
    case is_integer(Limit) andalso Limit > 0 of
        true -> Limit;
        false -> error(invalid_state_byte_limit)
    end.

check_size(_File, Size, Limit) when Size =< Limit -> ok;
check_size(File, _Size, Limit) -> error({state_file_too_large, File, Limit}).

-doc """
Writes and syncs a temporary file, then renames it over the target, so the
file is either the old or the new version, also after a crash.
Parent directories are not synced; this does not establish host power-loss durability.
""".
-spec save(file:filename(), term()) -> ok.
save(File, Term) ->
    Binary = term_to_binary(Term),
    check_size(File, byte_size(Binary), byte_limit()),
    write_result(File, ensure_directory, filelib:ensure_dir(File)),
    Temporary = File ++ ".tmp",
    Fd = case file:open(Temporary, [write, raw, binary]) of
        {ok, Opened} -> Opened;
        {error, Reason} -> error({state_file_write_failed, File, open, Reason})
    end,
    try
        write_result(File, write, file:write(Fd, Binary)),
        write_result(File, sync, file:sync(Fd))
    after
        write_result(File, close, file:close(Fd))
    end,
    write_result(File, rename, file:rename(Temporary, File)).

write_result(_File, _Operation, ok) -> ok;
write_result(File, Operation, {error, Reason}) -> error({state_file_write_failed, File, Operation, Reason}).
