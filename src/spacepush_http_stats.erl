-module(spacepush_http_stats).
-moduledoc """
`GET /v1/stats` returns the usage statistics as JSON (see `spacepush_stats`).

Only for requests made on the server itself: the peer must be a loopback
address and the request must not have passed a proxy, which adds
`X-Forwarded-For`. Everything else gets 404, as if the path did not exist.
`?days=N` sets how many days of daily counters are included (default 14,
at most 400).
""".
-behaviour(cowboy_handler).

-export([init/2, local/1]).

-define(DEFAULT_DAYS, 14).

init(Req0, Opts) ->
    Req =
        case {cowboy_req:method(Req0), local(Req0)} of
            {<<"GET">>, true} ->
                Body = json:encode(spacepush_stats:report(days(Req0))),
                spacepush_http:json_reply(200, #{<<"cache-control">> => <<"no-store">>}, Body, Req0);
            _ ->
                spacepush_http:error_reply(404, <<"not_found">>, Req0)
        end,
    {ok, Req, Opts}.

-doc "True for a request from a loopback address that no proxy forwarded.".
-spec local(cowboy_req:req()) -> boolean().
local(Req) ->
    {Ip, _Port} = cowboy_req:peer(Req),
    Forwarded = [Name || Name <- [<<"x-forwarded-for">>, <<"forwarded">>, <<"x-real-ip">>], cowboy_req:header(Name, Req) =/= undefined],
    Forwarded =:= [] andalso loopback(Ip).

loopback({127, _, _, _}) -> true;
loopback({0, 0, 0, 0, 0, 0, 0, 1}) -> true;
loopback({0, 0, 0, 0, 0, 16#ffff, A, _B}) -> A bsr 8 =:= 127;
loopback(_) -> false.

days(Req) ->
    try cowboy_req:match_qs([{days, int, ?DEFAULT_DAYS}], Req) of
        #{days := Days} -> Days
    catch
        _:_ -> ?DEFAULT_DAYS
    end.
