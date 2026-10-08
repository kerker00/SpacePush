-module(spacepush_store).
-moduledoc "Saves a term to a file atomically and durably, and reads it back.".

-include_lib("kernel/include/logger.hrl").

-export([load/2, save/2]).

-doc """
Returns the saved term, or `Default` if the file is missing or unreadable.

Decoding is `safe`, so it never creates atoms: the caller must load the module
that defines the atoms in the term before calling this.
""".
-spec load(file:filename(), term()) -> term().
load(File, Default) ->
    case file:read_file(File) of
        {ok, Binary} ->
            try
                binary_to_term(Binary, [safe])
            catch
                error:badarg ->
                    ?LOG_ERROR(#{msg => unreadable_state_file, file => File}),
                    Default
            end;
        {error, enoent} ->
            Default
    end.

-doc """
Writes and syncs a temporary file, then renames it over the target, so the
file is either the old or the new version, also after a crash.
""".
-spec save(file:filename(), term()) -> ok.
save(File, Term) ->
    ok = filelib:ensure_dir(File),
    Temporary = File ++ ".tmp",
    {ok, Fd} = file:open(Temporary, [write, raw, binary]),
    try
        ok = file:write(Fd, term_to_binary(Term)),
        ok = file:sync(Fd)
    after
        ok = file:close(Fd)
    end,
    ok = file:rename(Temporary, File).
