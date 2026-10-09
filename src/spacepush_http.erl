-module(spacepush_http).
-moduledoc "Helpers shared by the HTTP handlers.".

-export([client/1, error_reply/3, json_reply/4]).

-doc """
The client address for rate limiting. Behind a trusted proxy it is the last
X-Forwarded-For entry: the proxy appends the address it saw, while earlier
entries come from the client and can be forged.
""".
-spec client(cowboy_req:req()) -> term().
client(Req) ->
    Forwarded = cowboy_req:header(<<"x-forwarded-for">>, Req),
    case application:get_env(spacepush, trust_proxy, false) of
        true when is_binary(Forwarded) ->
            string:trim(lists:last(binary:split(Forwarded, <<",">>, [global])));
        _ ->
            {Ip, _Port} = cowboy_req:peer(Req),
            Ip
    end.

-spec error_reply(cowboy:http_status(), binary(), cowboy_req:req()) -> cowboy_req:req().
error_reply(Status, Reason, Req) ->
    json_reply(Status, #{}, json:encode(#{<<"error">> => Reason}), Req).

-spec json_reply(cowboy:http_status(), cowboy:http_headers(), iodata(), cowboy_req:req()) -> cowboy_req:req().
json_reply(Status, Headers, Body, Req) ->
    cowboy_req:reply(
        Status,
        Headers#{<<"content-type">> => <<"application/json">>, <<"x-content-type-options">> => <<"nosniff">>},
        Body,
        Req
    ).
