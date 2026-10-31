-module(spacepush_app).
-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

-define(LISTENER, spacepush_http).
-define(STREAM_LISTENER, spacepush_stream_http).

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
            {"/v1/test", spacepush_http_test, []},
            {"/v1/stats", spacepush_http_stats, api},
            {"/stats", spacepush_http_stats, dashboard},
            {"/stats/[...]", spacepush_http_stats, dashboard}
        ]}
    ]),
    {ok, Ip} = application:get_env(spacepush, http_ip),
    {ok, Port} = application:get_env(spacepush, http_port),
    {ok, MaxConnections} = application:get_env(spacepush, http_max_connections),
    TransportOptions = #{socket_opts => [{ip, Ip}, {port, Port}], max_connections => MaxConnections},
    {ok, _} = cowboy:start_clear(?LISTENER, TransportOptions, #{env => #{dispatch => Dispatch}}),
    start_stream_listener(),
    {ok, Sup}.

prep_stop(State) ->
    cowboy:stop_listener(?LISTENER),
    _ = cowboy:stop_listener(?STREAM_LISTENER),
    State.

stop(_State) ->
    ok.

%% Long-lived streams get their own listener, so they cannot use up the API's
%% connections. It is off unless `stream_http_port` is set, and a failure to
%% start it is logged without stopping the application.
start_stream_listener() ->
    case application:get_env(spacepush, stream_http_port, undefined) of
        undefined ->
            ok;
        Port ->
            {ok, Ip} = application:get_env(spacepush, stream_http_ip),
            {ok, MaxConnections} = application:get_env(spacepush, stream_max_connections),
            {ok, Lifetime} = application:get_env(spacepush, stream_max_lifetime_ms),
            Dispatch = cowboy_router:compile([
                {'_', [
                    {"/v1/stream", spacepush_http_stream, []},
                    {"/v1/stream/test", spacepush_http_stream_test, []}
                ]}
            ]),
            TransportOptions = #{socket_opts => [{ip, Ip}, {port, Port}], max_connections => MaxConnections},
            %% Heartbeats are sent, not received, so the idle timeout only has to outlast a stream.
            ProtocolOptions = #{env => #{dispatch => Dispatch}, idle_timeout => Lifetime + 60000},
            case cowboy:start_clear(?STREAM_LISTENER, TransportOptions, ProtocolOptions) of
                {ok, _} -> ok;
                {error, Reason} -> logger:error("stream listener not started: ~p", [Reason])
            end
    end.
