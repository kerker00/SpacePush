-module(spacepush_http_read).
-moduledoc """
Read API for the apps, in the shapes they already decode:

- `GET /v1/directory` – the listed spaces in the aggregator's format
  (`url`, `lastSeen`, `data.space`, `data.location`, `data.state.open`). The
  state comes from SpacePush's own recent fetch when there is one, otherwise
  from the hourly directory while its original observation is still recent.
  Expired observations keep their display fields and endpoint but omit the
  open flag. Directory and summary require HTTP cache revalidation.
- `GET /v1/spaces?endpoint=<url>` – the space's SpaceAPI document as fetched,
  with its fetch-time and age-limit headers; a refresh failure can fall back
  only while the last successful fetch is within `max_data_age_s`;
  404 for endpoints not in the directory.
- `GET /v1/mainframe/rooms` – Mainframe's openState response as fetched.
- `GET /v1/summary` – how many of the listed spaces are open, from the same
  states as the directory. `known` and `unknown` describe coverage; `as_of`
  is the oldest included observation, `newest_as_of` the newest, both null
  when no current state is known. Unknown spaces are not counted as closed.
  Public data, so any web page may read it (CORS `*`).

Answers 502 when a space cannot be fetched and nothing is cached, 503 when too
many spaces are being fetched at once, and 429 when a client exceeds
`read_rate_limit_per_minute`. Documents are passed through as fetched, so
responses carry `X-Content-Type-Options: nosniff`: a browser opening such a URL
must not render a hostile document as HTML.
""".
-behaviour(cowboy_handler).

-export([init/2, directory/1, summary/1]).

init(Req0, Kind) ->
    Req =
        case {cowboy_req:method(Req0), spacepush_ratelimit:allow(read, spacepush_http:client(Req0))} of
            {<<"GET">>, true} ->
                spacepush_stats:request(atom_to_binary(Kind), Req0),
                handle(Kind, Req0);
            {<<"GET">>, false} ->
                spacepush_stats:rate_limited(read),
                spacepush_http:error_reply(429, <<"rate_limited">>, Req0);
            {_Method, _Allowed} -> cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req0)
        end,
    {ok, Req, Kind}.

handle(directory, Req) ->
    case spacepush_directory:loaded() of
        true ->
            Body = json:encode(directory(erlang:system_time(millisecond))),
            spacepush_http:json_reply(200, read_headers(), Body, Req);
        false ->
            spacepush_http:error_reply(503, <<"directory_unavailable">>, Req)
    end;
handle(summary, Req) ->
    case spacepush_directory:loaded() of
        true ->
            Headers = (read_headers())#{<<"access-control-allow-origin">> => <<"*">>,
                <<"access-control-expose-headers">> => <<"X-SpacePush-Max-Data-Age">>},
            spacepush_http:json_reply(200, Headers, json:encode(summary(directory(erlang:system_time(millisecond)))), Req);
        false ->
            spacepush_http:error_reply(503, <<"directory_unavailable">>, Req)
    end;
handle(space, Req) ->
    case cowboy_req:match_qs([{endpoint, [], undefined}], Req) of
        #{endpoint := Endpoint} when is_binary(Endpoint) ->
            case spacepush_directory:known(Endpoint) of
                true -> serve(Endpoint, Req);
                false -> spacepush_http:error_reply(404, <<"unknown_space">>, Req)
            end;
        _ ->
            spacepush_http:error_reply(400, <<"missing_endpoint">>, Req)
    end;
handle(mainframe_rooms, Req) ->
    {ok, Url} = application:get_env(spacepush, mainframe_url),
    serve(unicode:characters_to_binary(Url), Req).

serve(Url, Req) ->
    try spacepush_cache:get(Url) of
        {ok, #{body := Body, fetched_at := FetchedAt}} ->
            {ok, MaxAge} = application:get_env(spacepush, max_data_age_s),
            Now = erlang:system_time(millisecond),
            case current(FetchedAt, Now, MaxAge * 1000) of
                true ->
                    Headers = (read_headers())#{
                        <<"x-spacepush-fetched-at">> => integer_to_binary(FetchedAt div 1000),
                        <<"x-spacepush-expires-at">> => integer_to_binary((FetchedAt + MaxAge * 1000) div 1000)
                    },
                    spacepush_http:json_reply(200, Headers, Body, Req);
                false ->
                    spacepush_http:error_reply(502, <<"stale_data">>, Req)
            end;
        {error, busy} ->
            spacepush_http:error_reply(503, <<"busy">>, Req);
        {error, _Reason} ->
            spacepush_http:error_reply(502, <<"space_unreachable">>, Req)
    catch
        exit:{timeout, _} -> spacepush_http:error_reply(504, <<"space_timeout">>, Req)
    end.

-doc "The directory as aggregator-shaped maps, with states from recent fetches where available.".
-spec directory(integer()) -> [map()].
directory(Now) ->
    {ok, MaxAge} = application:get_env(spacepush, max_data_age_s),
    [entry(Entry, Now, MaxAge * 1000) || #{name := Name} = Entry <- spacepush_directory:entries(), Name =/= null].

entry(#{endpoint := Url, name := Name, address := Address, lat := Lat, lon := Lon} = Entry, Now, MaxAge) ->
    LastSeen0 = maps:get(last_seen, Entry, null),
    AggregatorCurrent = is_number(LastSeen0) andalso current(LastSeen0 * 1000, Now, MaxAge),
    {Open, LastSeen} =
        case spacepush_cache:lookup(Url) of
            #{fetched_at := FetchedAt, observations := [{_Topic, _Name, State} | _]} ->
                %% Watching controls polling, not the age of an observation.
                %% Prefer the newer usable observation from either source.
                case current(FetchedAt, Now, MaxAge) andalso
                    (not AggregatorCurrent orelse FetchedAt >= LastSeen0 * 1000) of
                    true -> {open_flag(State), FetchedAt div 1000};
                    false -> {aggregator_open(Entry, AggregatorCurrent), LastSeen0}
                end;
            _ ->
                {aggregator_open(Entry, AggregatorCurrent), LastSeen0}
        end,
    Location = maps:filter(
        fun(_Key, Value) -> Value =/= null end,
        #{<<"address">> => Address, <<"lat">> => Lat, <<"lon">> => Lon}
    ),
    Data = without_empty(#{
        <<"space">> => Name,
        <<"location">> => Location,
        <<"state">> => maps:filter(fun(_Key, Value) -> Value =/= null end, #{<<"open">> => Open})
    }),
    maps:filter(fun(_Key, Value) -> Value =/= null end, #{<<"url">> => Url, <<"lastSeen">> => LastSeen, <<"data">> => Data}).

-doc "Counts the open spaces among directory entries as `directory/1` returns them.".
-spec summary([map()]) -> map().
summary(Entries) ->
    Open = length([E || #{<<"data">> := #{<<"state">> := #{<<"open">> := true}}} = E <- Entries]),
    Seen = [trunc(S) || #{<<"lastSeen">> := S, <<"data">> := #{<<"state">> := #{<<"open">> := O}}} <- Entries, is_number(S), is_boolean(O)],
    {AsOf, NewestAsOf} =
        case Seen of
            [] -> {null, null};
            _ -> {timestamp(lists:min(Seen)), timestamp(lists:max(Seen))}
        end,
    Known = length(Seen),
    #{<<"open">> => Open, <<"total">> => length(Entries), <<"known">> => Known,
        <<"unknown">> => length(Entries) - Known, <<"as_of">> => AsOf,
        <<"newest_as_of">> => NewestAsOf}.

current(ObservedAt, Now, MaxAge) ->
    ObservedAt =< Now andalso Now - ObservedAt =< MaxAge.

read_headers() ->
    {ok, MaxAge} = application:get_env(spacepush, max_data_age_s),
    #{<<"cache-control">> => <<"no-cache">>,
        <<"x-spacepush-max-data-age">> => integer_to_binary(MaxAge)}.

aggregator_open(Entry, true) -> maps:get(open, Entry, null);
aggregator_open(_Entry, false) -> null.

timestamp(Seen) ->
    list_to_binary(calendar:system_time_to_rfc3339(Seen, [{unit, second}, {offset, "Z"}])).

without_empty(Map) ->
    maps:filter(fun(_Key, Value) -> Value =/= #{} end, Map).

open_flag(open) -> true;
open_flag(closed) -> false;
open_flag(_) -> null.
