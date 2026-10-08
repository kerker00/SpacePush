-module(spacepush_store).
-moduledoc "Saves a term to a file atomically and reads it back.".

-include_lib("kernel/include/logger.hrl").

-export([load/2, save/2]).

-doc "Returns the saved term, or `Default` if the file is missing or unreadable.".
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

-doc "Writes to a temporary file first, so a crash never leaves a half-written file.".
-spec save(file:filename(), term()) -> ok.
save(File, Term) ->
    ok = filelib:ensure_dir(File),
    Temporary = File ++ ".tmp",
    ok = file:write_file(Temporary, term_to_binary(Term)),
    ok = file:rename(Temporary, File).
