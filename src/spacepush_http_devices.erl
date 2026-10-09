-module(spacepush_http_devices).
-moduledoc """
`PUT /v1/devices/:token` registers a device, `DELETE` removes it.

Registration body:

```json
{"environment": "sandbox",
 "platform": "ios",
 "subscriptions": [{"endpoint": "https://status.mainframe.io/api/spaceInfo", "room": "radstelle"}]}
```

`room` is optional and defaults to `"space"`. `platform` is `"ios"` or
`"macos"`; it is optional, for the statistics only. Every endpoint must be listed in
the SpaceAPI directory. Answers 204 on success, 400 for invalid input or an
unknown endpoint, 413 for a body over 16 KB, 408 if the body does not arrive
within `http_body_timeout_ms`, 429 when the client sent too many requests and
503 when the registry is full or the directory is not loaded yet.
""".
-behaviour(cowboy_handler).

-export([init/2, parse_registration/1, registration/1, valid_token/1]).

-define(MAX_BODY, 16384).
-define(MAX_SUBSCRIPTIONS, 50).
-define(MAX_ENDPOINT, 512).

init(Req0, Opts) ->
    spacepush_stats:request(kind(cowboy_req:method(Req0)), Req0),
    Req =
        case spacepush_ratelimit:allow(write, spacepush_http:client(Req0)) of
            true ->
                handle(cowboy_req:method(Req0), cowboy_req:binding(token, Req0), Req0);
            false ->
                spacepush_stats:rate_limited(write),
                spacepush_http:error_reply(429, <<"rate_limited">>, Req0)
        end,
    {ok, Req, Opts}.

kind(<<"PUT">>) -> <<"devices_put">>;
kind(<<"DELETE">>) -> <<"devices_delete">>;
kind(_) -> <<"devices_other">>.

handle(Method, Token, Req) ->
    case valid_token(Token) of
        true -> handle_valid(Method, string:lowercase(Token), Req);
        false -> spacepush_http:error_reply(400, <<"invalid_token">>, Req)
    end.

handle_valid(<<"PUT">>, Token, Req0) ->
    case read_body(Req0) of
        {ok, Body, Req} ->
            case registration(Body) of
                {ok, Environment, Topics, Platform} ->
                    register(Token, Environment, Topics, Platform, Req);
                {error, Reason} ->
                    spacepush_http:error_reply(400, Reason, Req)
            end;
        {error, too_large, Req} ->
            spacepush_http:error_reply(413, <<"body_too_large">>, Req);
        {error, timeout, Req} ->
            spacepush_http:error_reply(408, <<"body_timeout">>, Req)
    end;
handle_valid(<<"DELETE">>, Token, Req) ->
    ok = spacepush_registry:unregister(Token),
    cowboy_req:reply(204, Req);
handle_valid(_Method, _Token, Req) ->
    cowboy_req:reply(405, #{<<"allow">> => <<"PUT, DELETE">>}, Req).

%% Only endpoints from the directory: SpacePush fetches what devices subscribe
%% to, so it must not accept arbitrary, possibly internal, URLs.
register(Token, Environment, Topics, Platform, Req) ->
    Unknown = [Endpoint || {Endpoint, _Room} <- Topics, not spacepush_directory:known(Endpoint)],
    case {spacepush_directory:loaded(), Unknown} of
        {false, _} ->
            spacepush_http:error_reply(503, <<"directory_unavailable">>, Req);
        {true, [_ | _]} ->
            spacepush_http:error_reply(400, <<"unknown_space">>, Req);
        {true, []} ->
            case spacepush_registry:register(Token, Environment, Topics, Platform) of
                ok -> cowboy_req:reply(204, Req);
                {error, full} -> spacepush_http:error_reply(503, <<"registry_full">>, Req)
            end
    end.

%% Cowboy's `length` only sets how much to read per call, so the total size and
%% the overall time are checked here. A declared length over the limit is
%% rejected before reading.
read_body(Req) ->
    case cowboy_req:body_length(Req) of
        Length when is_integer(Length), Length > ?MAX_BODY -> {error, too_large, Req};
        _ ->
            {ok, Timeout} = application:get_env(spacepush, http_body_timeout_ms),
            read_body(Req, [], 0, erlang:monotonic_time(millisecond) + Timeout)
    end.

read_body(Req0, Acc, Size, Deadline) ->
    Period = max(0, Deadline - erlang:monotonic_time(millisecond)),
    {Status, Data, Req} = cowboy_req:read_body(Req0, #{length => ?MAX_BODY + 1, period => Period}),
    Size1 = Size + byte_size(Data),
    if
        Size1 > ?MAX_BODY -> {error, too_large, Req};
        Status =:= ok -> {ok, iolist_to_binary(lists:reverse(Acc, [Data])), Req};
        Period =:= 0 -> {error, timeout, Req};
        true -> read_body(Req, [Data | Acc], Size1, Deadline)
    end.

-doc "APNs device tokens are hex strings, currently 64 characters long.".
-spec valid_token(term()) -> boolean().
valid_token(Token) when is_binary(Token), byte_size(Token) >= 64, byte_size(Token) =< 200 ->
    re:run(Token, "^[0-9a-fA-F]+$", [{capture, none}]) =:= match;
valid_token(_) ->
    false.

-spec parse_registration(binary()) ->
    {ok, spacepush_registry:environment(), [spacepush_state:topic()]} | {error, binary()}.
parse_registration(Body) ->
    case registration(Body) of
        {ok, Environment, Topics, _Platform} -> {ok, Environment, Topics};
        {error, Reason} -> {error, Reason}
    end.

-doc "Like `parse_registration/1`, also returning the platform.".
-spec registration(binary()) ->
    {ok, spacepush_registry:environment(), [spacepush_state:topic()], spacepush_registry:platform()} | {error, binary()}.
registration(Body) ->
    try json:decode(Body) of
        #{<<"environment">> := Environment, <<"subscriptions">> := Subscriptions} = Registration when
            is_list(Subscriptions), length(Subscriptions) =< ?MAX_SUBSCRIPTIONS
        ->
            case {environment(Environment), topics(Subscriptions), platform(maps:get(<<"platform">>, Registration, null))} of
                {{ok, Env}, {ok, Topics}, {ok, Platform}} -> {ok, Env, Topics, Platform};
                {{error, Reason}, _, _} -> {error, Reason};
                {_, {error, Reason}, _} -> {error, Reason};
                {_, _, {error, Reason}} -> {error, Reason}
            end;
        _ ->
            {error, <<"invalid_registration">>}
    catch
        error:_ -> {error, <<"invalid_json">>}
    end.

environment(<<"sandbox">>) -> {ok, sandbox};
environment(<<"production">>) -> {ok, production};
environment(_) -> {error, <<"invalid_environment">>}.

platform(null) -> {ok, <<"unknown">>};
platform(<<"ios">>) -> {ok, <<"ios">>};
platform(<<"macos">>) -> {ok, <<"macos">>};
platform(_) -> {error, <<"invalid_platform">>}.

topics(Subscriptions) ->
    Topics = [topic(Subscription) || Subscription <- Subscriptions],
    case lists:member(error, Topics) of
        true -> {error, <<"invalid_subscription">>};
        false -> {ok, lists:usort(Topics)}
    end.

topic(#{<<"endpoint">> := Endpoint} = Subscription) when is_binary(Endpoint), byte_size(Endpoint) =< ?MAX_ENDPOINT ->
    Room = maps:get(<<"room">>, Subscription, <<"space">>),
    case valid_endpoint(Endpoint) andalso valid_room(Room) of
        true -> {Endpoint, Room};
        false -> error
    end;
topic(_) ->
    error.

valid_endpoint(Endpoint) ->
    case uri_string:parse(Endpoint) of
        #{scheme := Scheme, host := Host} when Scheme =:= <<"https">>; Scheme =:= <<"http">> ->
            Host =/= <<>>;
        _ ->
            false
    end.

valid_room(Room) when is_binary(Room), byte_size(Room) >= 1, byte_size(Room) =< 32 ->
    re:run(Room, "^[a-z0-9_]+$", [{capture, none}]) =:= match;
valid_room(_) ->
    false.
