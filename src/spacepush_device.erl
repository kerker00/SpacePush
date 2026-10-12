-module(spacepush_device).
-moduledoc "Bounded byte validation shared by HTTP, persisted registrations and APNs.".

-export([valid_token/1, valid_room/1]).

-spec valid_token(term()) -> boolean().
valid_token(Token) when is_binary(Token), byte_size(Token) >= 64, byte_size(Token) =< 200 ->
    hex(Token);
valid_token(_) ->
    false.

hex(<<>>) ->
    true;
hex(<<Byte, Rest/binary>>) when
    Byte >= $0, Byte =< $9;
    Byte >= $a, Byte =< $f;
    Byte >= $A, Byte =< $F
->
    hex(Rest);
hex(_) ->
    false.

-spec valid_room(term()) -> boolean().
valid_room(Room) when is_binary(Room), byte_size(Room) >= 1, byte_size(Room) =< 32 ->
    room(Room);
valid_room(_) ->
    false.

room(<<>>) ->
    true;
room(<<Byte, Rest/binary>>) when
    Byte >= $a, Byte =< $z;
    Byte >= $0, Byte =< $9;
    Byte =:= $_
->
    room(Rest);
room(_) ->
    false.
