-module(spacepush_http_stats).
-moduledoc """
`GET /v1/stats` returns usage statistics as JSON; `/stats/` serves a dashboard.

Only for requests made on the server itself: the peer must be a loopback
address and the request must not have passed a proxy, which adds
`X-Forwarded-For`. Everything else gets 404, as if the path did not exist.
Local requests additionally require HTTP Basic auth using `stats_username`
and `stats_password` from the external runtime configuration. Missing or
invalid configuration disables both endpoints (503); there is no default login.
`?days=N` sets how many days of daily counters are included (default 14,
at most 400).
""".
-behaviour(cowboy_handler).

-export([init/2, local/1]).

-define(DEFAULT_DAYS, 14).

init(Req0, Opts) ->
    Req = case local(Req0) of
        false -> error_reply(404, <<"not_found">>, #{}, Req0);
        true -> authenticate(Req0, Opts)
    end,
    {ok, Req, Opts}.

authenticate(Req, Opts) ->
    case credentials() of
        disabled -> error_reply(503, <<"stats_unavailable">>, #{}, Req);
        {User, Password} ->
            case authorized(Req, User, Password) of
                true -> serve(Req, Opts);
                false -> error_reply(401, <<"unauthorized">>, #{
                    <<"www-authenticate">> => <<"Basic realm=\"SpacePush statistics\", charset=\"UTF-8\"">>
                }, Req)
            end
    end.

%% Only the external sys.config contains actual credentials. Invalid/empty
%% values never fall back to a built-in account or permit anonymous access.
credentials() ->
    User = application:get_env(spacepush, stats_username, undefined),
    Password = application:get_env(spacepush, stats_password, undefined),
    case {User, Password} of
        {U, P} when is_binary(U), byte_size(U) > 0, byte_size(U) =< 256,
                    is_binary(P), byte_size(P) > 0, byte_size(P) =< 1024 ->
            case binary:match(U, <<":">>) of
                nomatch -> {U, P};
                _ -> disabled
            end;
        _ -> disabled
    end.

authorized(Req, User, Password) ->
    try cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, GivenUser, GivenPassword} ->
            %% Compare fixed-size digests in constant time, including the username.
            Expected = crypto:hash(sha256, [User, <<":">>, Password]),
            Given = crypto:hash(sha256, [GivenUser, <<":">>, GivenPassword]),
            crypto:hash_equals(Expected, Given);
        _ -> false
    catch
        %% Malformed credentials must never crash a request or enter the logs.
        _:_ -> false
    end.

serve(Req, Opts) ->
    case cowboy_req:method(Req) of
        <<"GET">> -> resource(Req, Opts);
        <<"HEAD">> -> resource(Req, Opts);
        _ -> error_reply(405, <<"method_not_allowed">>, #{<<"allow">> => <<"GET, HEAD">>}, Req)
    end.

resource(Req, api) -> report(Req);
resource(Req, dashboard) ->
    case cowboy_req:path(Req) of
        <<"/stats">> -> asset("index.html", <<"text/html; charset=utf-8">>, Req);
        <<"/stats/">> -> asset("index.html", <<"text/html; charset=utf-8">>, Req);
        %% Same protection space as the page, so browsers reuse its Basic login.
        <<"/stats/data.json">> -> report(Req);
        <<"/stats/dashboard.css">> -> asset("dashboard.css", <<"text/css; charset=utf-8">>, Req);
        <<"/stats/dashboard.js">> -> asset("dashboard.js", <<"text/javascript; charset=utf-8">>, Req);
        _ -> error_reply(404, <<"not_found">>, #{}, Req)
    end.

report(Req) ->
    Body = json:encode(spacepush_stats:report(days(Req))),
    spacepush_http:json_reply(200, headers(), Body, Req).

%% A fixed allowlist above, never a filesystem path supplied by the client.
asset(Name, Type, Req) ->
    File = filename:join([code:priv_dir(spacepush), "stats", Name]),
    case file:read_file(File) of
        {ok, Body} -> cowboy_req:reply(200, (headers())#{<<"content-type">> => Type}, Body, Req);
        {error, _} -> error_reply(503, <<"dashboard_unavailable">>, #{}, Req)
    end.

error_reply(Status, Reason, Extra, Req) ->
    spacepush_http:json_reply(Status, maps:merge(headers(), Extra), json:encode(#{<<"error">> => Reason}), Req).

headers() -> #{
    <<"cache-control">> => <<"no-store">>,
    <<"x-content-type-options">> => <<"nosniff">>,
    <<"referrer-policy">> => <<"no-referrer">>,
    <<"x-frame-options">> => <<"DENY">>,
    <<"content-security-policy">> => <<"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'">>
}.

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
