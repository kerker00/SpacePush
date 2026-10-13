-module(spacepush_persist).
-moduledoc "Bounded field checks for saved state; diagnostics never include complete records.".
-export([text/2, topic/1, counter/1, open_dets/2]).

text(Value, MaxBytes) when is_binary(Value), byte_size(Value) > 0, byte_size(Value) =< MaxBytes ->
    unicode:characters_to_binary(Value, utf8, utf8) =:= Value;
text(_, _) -> false.

topic({Endpoint, Room}) -> text(Endpoint, 4096) andalso spacepush_device:valid_room(Room);
topic(_) -> false.

counter(Value) -> is_integer(Value) andalso Value >= 0 andalso Value =< 16#7fffffffffffffff.

open_dets(Name, File) ->
    case filelib:ensure_dir(File) of
        ok -> ok;
        {error, Reason} -> error({state_directory_unwritable, File, Reason})
    end,
    case dets:open_file(Name, [{file, File}, {type, set}]) of
        {ok, Name} -> ok;
        {error, OpenReason} -> error({state_table_open_failed, File, OpenReason})
    end.
