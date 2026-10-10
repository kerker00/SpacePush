-module(spacepush_http_read).
-moduledoc """
Read API for the apps, in the shapes they already decode:

- `GET /v1/directory` – the listed spaces in the aggregator's format
  (`url`, `lastSeen`, `data.space`, `data.location`, `data.state.open`). The
  state comes from SpacePush's own recent fetch when there is one, otherwise
  from the hourly directory.
- `GET /v1/spaces?endpoint=<url>` – the space's SpaceAPI document as fetched,
  at most `cache_max_age_ms` old; 404 for endpoints not in the directory.
- `GET /v1/mainframe/rooms` – Mainframe's openState response as fetched.
- `GET /v1/summary` – how many of the listed spaces are open, from the same
  states as the directory: `{"open", "total", "as_of"}`, where `as_of` is the
  newest state. Public data, so any web page may read it (CORS `*`).

Answers 502 when a space cannot be fetched and nothing is cached, 503 when too
many spaces are being fetched at once, and 429 when a client exceeds
`read_rate_limit_per_minute`. Documents are passed through as fetched, so
responses carry `X-Content-Type-Options: nosniff`: a browser opening such a URL
must not render a hostile document as HTML.
""".
-behaviour(cowboy_handler).

-export([init/2, directory/1, summary/1]).

init(Req0, Kind) ->
    spacepush_stats:request(atom_to_binary(Kind), Req0),
    Req =
        case {cowboy_req:method(Req0), spacepush_ratelimit:allow(read, spacepush_http:client(Req0))} of
            {<<"GET">>, true} ->
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
            spacepush_http:json_reply(200, #{<<"cache-control">> => <<"max-age=300">>}, Body, Req);
        false ->
            spacepush_http:error_reply(503, <<"directory_unavailable">>, Req)
    end;
handle(summary, Req) ->
    case spacepush_directory:loaded() of
        true ->
            Headers = #{<<"cache-control">> => <<"max-age=60">>, <<"access-control-allow-origin">> => <<"*">>},
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
            Headers = #{
                <<"cache-control">> => <<"max-age=30">>,
                <<"x-spacepush-fetched-at">> => integer_to_binary(FetchedAt div 1000)
            },
            spacepush_http:json_reply(200, Headers, Body, Req);
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
    {ok, Window} = application:get_env(spacepush, watch_window_ms),
    [entry(Entry, Now, Window) || #{name := Name} = Entry <- spacepush_directory:entries(), Name =/= null].

entry(#{endpoint := Url, name := Name, address := Address, lat := Lat, lon := Lon} = Entry, Now, Window) ->
    {Open, LastSeen} =
        case spacepush_cache:lookup(Url) of
            #{fetched_at := FetchedAt, observations := [{_Topic, _Name, State} | _]} when Now - FetchedAt < Window ->
                {open_flag(State), FetchedAt div 1000};
            _ ->
                {maps:get(open, Entry), maps:get(last_seen, Entry)}
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
    Seen = [S || #{<<"lastSeen">> := S} <- Entries, is_integer(S)],
    AsOf =
        case Seen of
            [] -> null;
            _ -> list_to_binary(calendar:system_time_to_rfc3339(lists:max(Seen), [{unit, second}, {offset, "Z"}]))
        end,
    #{<<"open">> => Open, <<"total">> => length(Entries), <<"as_of">> => AsOf}.

without_empty(Map) ->
    maps:filter(fun(_Key, Value) -> Value =/= #{} end, Map).

open_flag(open) -> true;
open_flag(closed) -> false;
open_flag(_) -> null.
