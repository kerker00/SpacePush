-module(spacepush_notification).
-moduledoc """
Builds the APNs payload for a state change.

The body is a localization key, so the apps show it in the user's language.
The apps' string catalogs must define every key returned by `loc_key/1`.
""".

-export([payload/3, loc_key/1, collapse_id/1]).

-spec payload(spacepush_state:topic(), binary(), spacepush_state:state()) -> map().
payload({Endpoint, Room}, Name, State) ->
    #{
        <<"aps">> => #{
            <<"alert">> => #{
                <<"title">> => title(Name, Room),
                <<"loc-key">> => loc_key(State)
            },
            <<"sound">> => <<"default">>,
            <<"thread-id">> => Endpoint
        },
        <<"endpoint">> => Endpoint,
        <<"room">> => Room,
        <<"state">> => atom_to_binary(State)
    }.

-spec loc_key(spacepush_state:state()) -> binary().
loc_key(open) -> <<"PUSH_STATE_OPEN">>;
loc_key(closed) -> <<"PUSH_STATE_CLOSED">>;
loc_key(keyholder) -> <<"PUSH_STATE_KEYHOLDER">>;
loc_key(member) -> <<"PUSH_STATE_MEMBER">>;
loc_key(open_plus) -> <<"PUSH_STATE_OPEN_PLUS">>;
loc_key(closing) -> <<"PUSH_STATE_CLOSING">>.

title(Name, <<"space">>) -> Name;
title(Name, Room) -> <<Name/binary, " · "/utf8, (room_name(Room))/binary>>.

room_name(<<"radstelle">>) -> <<"Radstelle">>;
room_name(<<"lab3d">>) -> <<"3D Lab">>;
room_name(<<"machining">>) -> <<"Machining">>;
room_name(<<"woodworking">>) -> <<"Woodworking">>;
room_name(Room) -> Room.

-doc "Lets a newer notification about the same topic replace an older one on the device.".
-spec collapse_id(spacepush_state:topic()) -> binary().
collapse_id({Endpoint, Room}) ->
    binary:encode_hex(crypto:hash(sha256, <<Endpoint/binary, "#", Room/binary>>), lowercase).
