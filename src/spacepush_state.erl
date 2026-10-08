-module(spacepush_state).
-moduledoc """
Turns SpaceAPI aggregator and Mainframe openState responses into comparable
states, and finds the changes between two polls.

A topic is `{Endpoint, Room}`. Ordinary spaces only have the room `<<"space">>`.
Mainframe Oldenburg is read from its openState endpoint instead of the
aggregator, because it reports several rooms and finer states.
""".

-export([parse_aggregator/1, parse_mainframe/1, changes/2, mainframe_state/1]).

-export_type([topic/0, state/0, observation/0, change/0]).

-type topic() :: {Endpoint :: binary(), Room :: binary()}.
-type state() :: open | closed | keyholder | member | open_plus | closing | unknown.
-type observation() :: {topic(), Name :: binary(), state()}.
-type change() :: {topic(), Name :: binary(), From :: state(), To :: state()}.

-define(MAINFRAME_ENDPOINT, <<"https://status.mainframe.io/api/spaceInfo">>).
-define(MAINFRAME_HOST, <<"status.mainframe.io">>).
-define(SPACE_ROOM, <<"space">>).

-doc "Reads every space from an api.spaceapi.io response, except Mainframe.".
-spec parse_aggregator(binary()) -> [observation()].
parse_aggregator(Body) ->
    [Observation || Entry <- json:decode(Body), {ok, Observation} <- [aggregator_entry(Entry)]].

aggregator_entry(#{<<"url">> := Url, <<"data">> := #{<<"space">> := Name} = Data}) when
    is_binary(Url), is_binary(Name)
->
    case is_mainframe(Url) of
        true -> skip;
        false -> {ok, {{Url, ?SPACE_ROOM}, Name, open_flag(Data)}}
    end;
aggregator_entry(_) ->
    skip.

%% Schema v14+ keeps the flag in `state`; v0.13 had it at the top level.
open_flag(#{<<"state">> := #{<<"open">> := Open}}) when is_boolean(Open) -> bool_state(Open);
open_flag(#{<<"state">> := State}) when is_map(State) -> unknown;
open_flag(#{<<"open">> := Open}) when is_boolean(Open) -> bool_state(Open);
open_flag(_) -> unknown.

bool_state(true) -> open;
bool_state(false) -> closed.

is_mainframe(Url) ->
    case uri_string:parse(Url) of
        #{host := Host} -> string:lowercase(Host) =:= ?MAINFRAME_HOST;
        _ -> false
    end.

-doc "Reads the rooms from Mainframe's openState response.".
-spec parse_mainframe(binary()) -> [observation()].
parse_mainframe(Body) ->
    [
        {{?MAINFRAME_ENDPOINT, Room}, <<"Mainframe">>, mainframe_state(Raw)}
     || Room := #{<<"state">> := Raw} <- json:decode(Body), is_binary(Room), is_binary(Raw)
    ].

-doc "Maps a state of ktt-ol/spacestatus2, including its legacy values.".
-spec mainframe_state(binary()) -> state().
mainframe_state(Raw) when Raw =:= <<"none">>; Raw =:= <<"off">>; Raw =:= <<"closed">> -> closed;
mainframe_state(<<"keyholder">>) -> keyholder;
mainframe_state(<<"member">>) -> member;
mainframe_state(Raw) when Raw =:= <<"open">>; Raw =:= <<"on">>; Raw =:= <<"opened">> -> open;
mainframe_state(<<"open+">>) -> open_plus;
mainframe_state(<<"closing">>) -> closing;
mainframe_state(_) -> unknown.

-doc """
Compares new observations with the last known states.

`unknown` never replaces a known state, so an endpoint that is briefly down
does not cause notifications. A topic seen for the first time is recorded
without a change.
""".
-spec changes(#{topic() => state()}, [observation()]) -> {#{topic() => state()}, [change()]}.
changes(Known, Observations) ->
    {Known1, Changes} = lists:foldl(fun change/2, {Known, []}, Observations),
    {Known1, lists:reverse(Changes)}.

change({_Topic, _Name, unknown}, Acc) ->
    Acc;
change({Topic, Name, State}, {Known, Changes}) ->
    case Known of
        #{Topic := State} -> {Known, Changes};
        #{Topic := Old} -> {Known#{Topic := State}, [{Topic, Name, Old, State} | Changes]};
        #{} -> {Known#{Topic => State}, Changes}
    end.
