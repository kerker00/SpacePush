-module(spacepush_app).
-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

-define(LISTENER, spacepush_http).

start(_StartType, _StartArgs) ->
    {ok, Sup} = spacepush_sup:start_link(),
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/v1/devices/:token", spacepush_http_devices, []},
            {"/v1/directory", spacepush_http_read, directory},
            {"/v1/summary", spacepush_http_read, summary},
            {"/v1/spaces", spacepush_http_read, space},
            {"/v1/mainframe/rooms", spacepush_http_read, mainframe_rooms},
            {"/health", spacepush_http_health, []},
            {"/v1/stats", spacepush_http_stats, []}
        ]}
    ]),
    {ok, Ip} = application:get_env(spacepush, http_ip),
    {ok, Port} = application:get_env(spacepush, http_port),
    {ok, MaxConnections} = application:get_env(spacepush, http_max_connections),
    TransportOptions = #{socket_opts => [{ip, Ip}, {port, Port}], max_connections => MaxConnections},
    {ok, _} = cowboy:start_clear(?LISTENER, TransportOptions, #{env => #{dispatch => Dispatch}}),
    {ok, Sup}.

prep_stop(State) ->
    cowboy:stop_listener(?LISTENER),
    State.

stop(_State) ->
    ok.
