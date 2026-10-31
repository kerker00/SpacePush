-module(spacepush_http_stream).
-moduledoc """
`GET /v1/stream?sub=<endpoint>&sub=<endpoint>|<room>` – confirmed state changes
of the subscribed spaces and rooms as server-sent events.

Each `sub` names a directory endpoint, optionally followed by `|` and a room
(default `space`), at most `stream_max_topics` of them. Invalid input answers
400, an unknown endpoint 404, a client over `read_rate_limit_per_minute` or
`stream_max_per_client` 429, and an unloaded directory 503, all before the
stream starts.

The stream sends a `retry:` hint, then `event: change` (and `event: test`)
with JSON data, and a `: ping` comment every `stream_heartbeat_ms` so proxies
keep the connection. It ends after `stream_max_lifetime_ms`, or when the
`spacepush_stream` scope restarts; clients reconnect and read the current state
through the read API. Nothing is replayed.
""".
-behaviour(cowboy_loop).

-export([init/2, info/3, subscriptions/1]).

init(Req0, Opts) ->
    case cowboy_req:method(Req0) of
        <<"GET">> ->
            Client = spacepush_http:client(Req0),
            case spacepush_ratelimit:allow(read, Client) of
                true -> start(Client, Req0, Opts);
                false ->
                    spacepush_stats:rate_limited(read),
                    {ok, spacepush_http:error_reply(429, <<"rate_limited">>, Req0), Opts}
            end;
        _ ->
            {ok, cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req0), Opts}
    end.

start(Client, Req0, Opts) ->
    case check(Req0) of
        {ok, Topics} ->
            case spacepush_stream:subscribe(Topics, Client) of
                ok -> open(Req0, Opts);
                {error, too_many_streams} ->
                    spacepush_stats:count(<<"stream.rejected">>),
                    {ok, spacepush_http:error_reply(429, <<"too_many_streams">>, Req0), Opts}
            end;
        {error, Status, Reason} ->
            {ok, spacepush_http:error_reply(Status, Reason, Req0), Opts}
    end.

check(Req) ->
    case subscriptions(cowboy_req:parse_qs(Req)) of
        error -> {error, 400, <<"invalid_subscription">>};
        {ok, Topics} ->
            case spacepush_directory:loaded() of
                false -> {error, 503, <<"directory_unavailable">>};
                true ->
                    case lists:all(fun({Endpoint, _Room}) -> spacepush_directory:known(Endpoint) end, Topics) of
                        true -> {ok, Topics};
                        false -> {error, 404, <<"unknown_space">>}
                    end
            end
    end.

-doc "Parses the `sub` parameters into distinct topics; other parameters are ignored.".
-spec subscriptions([{binary(), binary() | true}]) -> {ok, [spacepush_state:topic()]} | error.
subscriptions(Query) ->
    {ok, Max} = application:get_env(spacepush, stream_max_topics),
    Subs = [Value || {<<"sub">>, Value} <- Query],
    Topics = lists:usort([topic(Sub) || Sub <- Subs]),
    case Subs =/= [] andalso length(Subs) =< Max andalso not lists:member(error, Topics) of
        true -> {ok, Topics};
        false -> error
    end.

topic(Sub) when is_binary(Sub), byte_size(Sub) > 0, byte_size(Sub) =< 4200 ->
    {Endpoint, Room} =
        case binary:split(Sub, <<"|">>) of
            [E] -> {E, <<"space">>};
            [E, R] -> {E, R}
        end,
    case byte_size(Endpoint) > 0 andalso byte_size(Endpoint) =< 4096 andalso spacepush_device:valid_room(Room) of
        true -> {Endpoint, Room};
        false -> error
    end;
topic(_) ->
    error.

open(Req0, _Opts) ->
    spacepush_stats:request(<<"stream">>, Req0),
    spacepush_stats:count(<<"stream.connects">>),
    Req = cowboy_req:stream_reply(200, #{
        <<"content-type">> => <<"text/event-stream">>,
        <<"cache-control">> => <<"no-store">>,
        <<"x-accel-buffering">> => <<"no">>,
        <<"x-content-type-options">> => <<"nosniff">>
    }, Req0),
    ok = cowboy_req:stream_body(<<"retry: ", (integer_to_binary(env(stream_retry_ms)))/binary, "\n\n">>, nofin, Req),
    Scope = erlang:monitor(process, spacepush_stream),
    erlang:send_after(env(stream_heartbeat_ms), self(), heartbeat),
    erlang:send_after(env(stream_max_lifetime_ms), self(), lifetime),
    {cowboy_loop, Req, #{scope => Scope}, hibernate}.

info({spacepush_stream, Event}, Req, State) ->
    ok = cowboy_req:stream_body(Event, nofin, Req),
    {ok, Req, State, hibernate};
info(heartbeat, Req, State) ->
    ok = cowboy_req:stream_body(<<": ping\n\n">>, nofin, Req),
    erlang:send_after(env(stream_heartbeat_ms), self(), heartbeat),
    {ok, Req, State, hibernate};
info(lifetime, Req, State) ->
    finish(Req, State);
info({'DOWN', Ref, process, _, _}, Req, #{scope := Ref} = State) ->
    finish(Req, State);
info(_Other, Req, State) ->
    {ok, Req, State, hibernate}.

finish(Req, State) ->
    ok = cowboy_req:stream_body(<<>>, fin, Req),
    {stop, Req, State}.

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
