-module(spacepush_http_devices).
-moduledoc """
`PUT /v1/devices/:token` registers a device, `DELETE` removes it.

Registration body:

```json
{"environment": "sandbox",
 "subscriptions": [{"endpoint": "https://status.mainframe.io/api/spaceInfo", "room": "radstelle"}]}
```

`room` is optional and defaults to `"space"`. Answers 204 on success, 413 for
a body over 16 KB, 408 if the body does not arrive within `http_body_timeout_ms`, 429
when the client sent too many requests and 503 when the registry is full.
""".
-behaviour(cowboy_handler).

-export([init/2, parse_registration/1, valid_token/1]).

-define(MAX_BODY, 16384).
-define(MAX_SUBSCRIPTIONS, 50).
-define(MAX_ENDPOINT, 512).

init(Req0, Opts) ->
    Req =
        case spacepush_ratelimit:allow(client(Req0)) of
            true -> handle(cowboy_req:method(Req0), cowboy_req:binding(token, Req0), Req0);
            false -> error_reply(429, <<"rate_limited">>, Req0)
        end,
    {ok, Req, Opts}.

handle(Method, Token, Req) ->
    case valid_token(Token) of
        true -> handle_valid(Method, string:lowercase(Token), Req);
        false -> error_reply(400, <<"invalid_token">>, Req)
    end.

handle_valid(<<"PUT">>, Token, Req0) ->
    case read_body(Req0) of
        {ok, Body, Req} ->
            case parse_registration(Body) of
                {ok, Environment, Topics} ->
                    case spacepush_registry:register(Token, Environment, Topics) of
                        ok -> cowboy_req:reply(204, Req);
                        {error, full} -> error_reply(503, <<"registry_full">>, Req)
                    end;
                {error, Reason} ->
                    error_reply(400, Reason, Req)
            end;
        {error, too_large, Req} ->
            error_reply(413, <<"body_too_large">>, Req);
        {error, timeout, Req} ->
            error_reply(408, <<"body_timeout">>, Req)
    end;
handle_valid(<<"DELETE">>, Token, Req) ->
    ok = spacepush_registry:unregister(Token),
    cowboy_req:reply(204, Req);
handle_valid(_Method, _Token, Req) ->
    cowboy_req:reply(405, #{<<"allow">> => <<"PUT, DELETE">>}, Req).

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
    try json:decode(Body) of
        #{<<"environment">> := Environment, <<"subscriptions">> := Subscriptions} when
            is_list(Subscriptions), length(Subscriptions) =< ?MAX_SUBSCRIPTIONS
        ->
            case {environment(Environment), topics(Subscriptions)} of
                {{ok, Env}, {ok, Topics}} -> {ok, Env, Topics};
                {{error, Reason}, _} -> {error, Reason};
                {_, {error, Reason}} -> {error, Reason}
            end;
        _ ->
            {error, <<"invalid_registration">>}
    catch
        error:_ -> {error, <<"invalid_json">>}
    end.

environment(<<"sandbox">>) -> {ok, sandbox};
environment(<<"production">>) -> {ok, production};
environment(_) -> {error, <<"invalid_environment">>}.

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

client(Req) ->
    Forwarded = cowboy_req:header(<<"x-forwarded-for">>, Req),
    case application:get_env(spacepush, trust_proxy, false) of
        true when is_binary(Forwarded) ->
            [First | _] = binary:split(Forwarded, <<",">>),
            string:trim(First);
        _ ->
            {Ip, _Port} = cowboy_req:peer(Req),
            Ip
    end.

error_reply(Status, Reason, Req) ->
    cowboy_req:reply(
        Status,
        #{<<"content-type">> => <<"application/json">>},
        json:encode(#{<<"error">> => Reason}),
        Req
    ).
