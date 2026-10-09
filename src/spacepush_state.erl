-module(spacepush_state).
-moduledoc """
Reads the SpaceAPI directory, single SpaceAPI documents and Mainframe's
openState response, and decides when an observed change counts as confirmed.

A topic is `{Endpoint, Room}`. Ordinary spaces only have the room `<<"space">>`.
Mainframe Oldenburg's state comes from its openState endpoint instead of its
SpaceAPI document, because it reports several rooms and finer states.
""".

-export([parse_directory/3, parse_space/2, parse_mainframe/1, mainframe_endpoint/0, mainframe_state/1]).
-export([track/4, snapshot/1, restore/1]).

-export_type([topic/0, state/0, observation/0, change/0, tracker/0, directory_entry/0]).

-type topic() :: {Endpoint :: binary(), Room :: binary()}.
-type state() :: open | closed | keyholder | member | open_plus | closing | unknown.
-type observation() :: {topic(), Name :: binary(), state()}.
-type change() :: {topic(), Name :: binary(), From :: state(), To :: state()}.
-type entry() :: #{confirmed := state(), candidate := none | {state(), Since :: integer()}}.
-type tracker() :: #{topic() => entry()}.
-type directory_entry() :: #{
    endpoint := binary(),
    name := binary() | null,
    address := binary() | null,
    lat := number() | null,
    lon := number() | null,
    open := boolean() | null,
    last_seen := number() | null
}.

-define(STATES, [open, closed, keyholder, member, open_plus, closing]).
-define(MAINFRAME_ENDPOINT, <<"https://status.mainframe.io/api/spaceInfo">>).
-define(SPACE_ROOM, <<"space">>).

-spec mainframe_endpoint() -> binary().
mainframe_endpoint() -> ?MAINFRAME_ENDPOINT.

-doc """
Reads the list of spaces from an api.spaceapi.io response.

Every listed endpoint is kept, also without data: the list is the allowlist of
endpoints SpacePush fetches. Name, address and open flag are only for display;
the flag counts only while the aggregator reached the endpoint recently.
`valid` is ignored on purpose: a schema error in an unrelated field does not
make the open flag wrong.
""".
-spec parse_directory(binary(), Now :: integer(), MaxAge :: integer()) -> [directory_entry()].
parse_directory(Body, Now, MaxAge) ->
    [Entry || Raw <- entries(json:decode(Body)), {ok, Entry} <- [directory_entry(Raw, Now, MaxAge)]].

%% The service answers with an array; its OpenAPI document describes an object with `items`.
entries(Entries) when is_list(Entries) -> Entries;
entries(#{<<"items">> := Entries}) when is_list(Entries) -> Entries.

directory_entry(#{<<"url">> := Url} = Raw, Now, MaxAge) when is_binary(Url) ->
    Data = map_or_empty(maps:get(<<"data">>, Raw, #{})),
    Location = map_or_empty(maps:get(<<"location">>, Data, #{})),
    Open =
        case fresh(Raw, Now, MaxAge) andalso open_flag(Data) of
            open -> true;
            closed -> false;
            _ -> null
        end,
    {ok, #{
        endpoint => Url,
        name => binary_or_null(maps:get(<<"space">>, Data, null)),
        address => binary_or_null(maps:get(<<"address">>, Location, null)),
        lat => number_or_null(maps:get(<<"lat">>, Location, null)),
        lon => number_or_null(maps:get(<<"lon">>, Location, null)),
        open => Open,
        last_seen => number_or_null(maps:get(<<"lastSeen">>, Raw, null))
    }};
directory_entry(_Raw, _Now, _MaxAge) ->
    skip.

map_or_empty(Map) when is_map(Map) -> Map;
map_or_empty(_) -> #{}.

binary_or_null(Value) when is_binary(Value) -> Value;
binary_or_null(_) -> null.

number_or_null(Value) when is_number(Value) -> Value;
number_or_null(_) -> null.

-doc """
Reads a space's own SpaceAPI document. Fails for anything that is not a JSON
object with a `space` name, so only usable documents reach the cache.
""".
-spec parse_space(binary(), binary()) -> observation().
parse_space(Endpoint, Body) ->
    case json:decode(Body) of
        #{<<"space">> := Name} = Data when is_binary(Name) ->
            {{Endpoint, ?SPACE_ROOM}, Name, open_flag(Data)};
        _ ->
            error(not_a_space)
    end.

fresh(#{<<"validationResult">> := #{<<"reachable">> := false}}, _Now, _MaxAge) -> false;
fresh(#{<<"lastSeen">> := LastSeen}, Now, MaxAge) when is_number(LastSeen) -> Now - LastSeen =< MaxAge;
fresh(_, _Now, _MaxAge) -> false.

%% Since v0.13 the flag lives in `state`; v0.12 and older kept it at the top level.
open_flag(#{<<"state">> := #{<<"open">> := Open}}) when is_boolean(Open) -> bool_state(Open);
open_flag(#{<<"state">> := State}) when is_map(State) -> unknown;
open_flag(#{<<"open">> := Open}) when is_boolean(Open) -> bool_state(Open);
open_flag(_) -> unknown.

bool_state(true) -> open;
bool_state(false) -> closed.

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
Applies one poll's observations to the tracker and returns the confirmed changes.

A topic seen for the first time is recorded without a change. A different
state becomes a candidate; it is confirmed, and reported, only when a later
observation still shows it at least `Debounce` seconds after it first appeared.
Every new candidate restarts that wait, and returning to the confirmed state
drops the candidate. `unknown` observations change nothing, so a candidate
cannot be confirmed without fresh data.
""".
-spec track(tracker(), [observation()], Now :: integer(), Debounce :: integer()) -> {tracker(), [change()]}.
track(Tracker, Observations, Now, Debounce) ->
    {Tracker1, Changes} = lists:foldl(
        fun(Observation, Acc) -> step(Observation, Acc, Now, Debounce) end,
        {Tracker, []},
        Observations
    ),
    {Tracker1, lists:reverse(Changes)}.

step({_Topic, _Name, unknown}, Acc, _Now, _Debounce) ->
    Acc;
step({Topic, Name, State}, {Tracker, Changes}, Now, Debounce) ->
    case Tracker of
        #{Topic := #{confirmed := State} = Entry} ->
            {Tracker#{Topic := Entry#{candidate := none}}, Changes};
        #{Topic := #{confirmed := Old, candidate := {State, Since}} = Entry} when Now - Since >= Debounce ->
            {Tracker#{Topic := Entry#{confirmed := State, candidate := none}}, [{Topic, Name, Old, State} | Changes]};
        #{Topic := #{candidate := {State, _Since}}} ->
            {Tracker, Changes};
        #{Topic := Entry} ->
            {Tracker#{Topic := Entry#{candidate := {State, Now}}}, Changes};
        #{} ->
            {Tracker#{Topic => #{confirmed => State, candidate => none}}, Changes}
    end.

-doc "Wraps the tracker in a versioned term for saving.".
-spec snapshot(tracker()) -> {tracker, 1, tracker()}.
snapshot(Tracker) ->
    {tracker, 1, Tracker}.

-doc """
Reads a saved snapshot, keeping only well-formed entries. Anything else,
including an unknown format version, gives an empty tracker.
""".
-spec restore(term()) -> tracker().
restore({tracker, 1, Tracker}) when is_map(Tracker) ->
    maps:filter(fun valid_entry/2, Tracker);
restore(_) ->
    #{}.

valid_entry({Endpoint, Room}, #{confirmed := Confirmed, candidate := Candidate}) when
    is_binary(Endpoint), is_binary(Room)
->
    lists:member(Confirmed, ?STATES) andalso valid_candidate(Candidate);
valid_entry(_Topic, _Entry) ->
    false.

valid_candidate(none) -> true;
valid_candidate({State, Since}) when is_integer(Since) -> lists:member(State, ?STATES);
valid_candidate(_) -> false.
