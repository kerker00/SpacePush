-module(spacepush_app).
-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

-define(LISTENER, spacepush_http).

start(_StartType, _StartArgs) ->
    {ok, Sup} = spacepush_sup:start_link(),
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/v1/devices/:token", spacepush_http_devices, []},
            {"/health", spacepush_http_health, []}
        ]}
    ]),
    {ok, Ip} = application:get_env(spacepush, http_ip),
    {ok, Port} = application:get_env(spacepush, http_port),
    {ok, _} = cowboy:start_clear(?LISTENER, [{ip, Ip}, {port, Port}], #{env => #{dispatch => Dispatch}}),
    {ok, Sup}.

prep_stop(State) ->
    cowboy:stop_listener(?LISTENER),
    State.

stop(_State) ->
    ok.
