-module(spacepush_http_health).
-moduledoc "`GET /health` answers 200 while the service runs, for uptime checks.".
-behaviour(cowboy_handler).

-export([init/2]).

init(Req0, Opts) ->
    Req = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"ok">>, Req0),
    {ok, Req, Opts}.
