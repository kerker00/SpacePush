-module(spacepush_http_tests).
-include_lib("eunit/include/eunit.hrl").

req(Forwarded) ->
    Headers =
        case Forwarded of
            undefined -> #{};
            _ -> #{<<"x-forwarded-for">> => Forwarded}
        end,
    #{headers => Headers, peer => {{10, 0, 0, 1}, 4711}}.

client_test_() ->
    {foreach, fun() -> application:load(spacepush) end, fun(_) -> application:set_env(spacepush, trust_proxy, false) end, [
        fun() ->
            application:set_env(spacepush, trust_proxy, false),
            ?assertEqual({10, 0, 0, 1}, spacepush_http:client(req(<<"1.2.3.4">>)))
        end,
        fun() ->
            application:set_env(spacepush, trust_proxy, true),
            %% The proxy appends what it saw; anything before that is the client's claim.
            ?assertEqual(<<"203.0.113.7">>, spacepush_http:client(req(<<"6.6.6.6, 203.0.113.7">>))),
            ?assertEqual(<<"203.0.113.7">>, spacepush_http:client(req(<<"203.0.113.7">>))),
            ?assertEqual({10, 0, 0, 1}, spacepush_http:client(req(undefined)))
        end
    ]}.
