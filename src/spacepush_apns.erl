-module(spacepush_apns).
-moduledoc """
Sends notifications to APNs over HTTP/2 with token-based authentication.

Keeps one connection per APNs environment open, as Apple recommends, and
renews the provider token before it expires after an hour. Device tokens
that APNs reports as no longer valid are removed from the registry.

Without a configured key file, notifications are only logged.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, push/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% Apple rejects provider tokens older than an hour.
-define(TOKEN_LIFETIME_S, 50 * 60).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec push(binary(), spacepush_registry:environment(), map()) -> ok.
push(Token, Environment, Payload) ->
    gen_server:cast(?MODULE, {push, Token, Environment, Payload}).

init([]) ->
    Key =
        case env(apns_key_file) of
            undefined ->
                ?LOG_WARNING("No APNs key configured, notifications are only logged"),
                undefined;
            File ->
                spacepush_jwt:read_key(File)
        end,
    {ok, #{
        key => Key,
        key_id => env(apns_key_id),
        team_id => env(apns_team_id),
        topic => env(apns_topic),
        provider_token => undefined,
        connections => #{},
        streams => #{}
    }}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({push, Token, Environment, Payload}, #{key := undefined} = State) ->
    ?LOG_INFO(#{msg => dry_run_push, token => Token, environment => Environment, payload => Payload}),
    {noreply, State};
handle_cast({push, Token, Environment, Payload}, State0) ->
    {ProviderToken, State1} = provider_token(State0),
    {Connection, State2} = connection(Environment, State1),
    #{topic := Topic, streams := Streams} = State2,
    Headers = [
        {<<"authorization">>, <<"bearer ", ProviderToken/binary>>},
        {<<"apns-topic">>, Topic},
        {<<"apns-push-type">>, <<"alert">>},
        {<<"apns-priority">>, <<"10">>}
    ],
    Ref = gun:post(Connection, <<"/3/device/", Token/binary>>, Headers, json:encode(Payload)),
    Stream = #{token => Token, status => undefined, body => <<>>},
    {noreply, State2#{streams := Streams#{Ref => Stream}}}.

handle_info({gun_response, _Conn, Ref, IsFin, Status, _Headers}, State) ->
    {noreply, update_stream(Ref, IsFin, fun(Stream) -> Stream#{status := Status} end, State)};
handle_info({gun_data, _Conn, Ref, IsFin, Data}, State) ->
    {noreply,
        update_stream(Ref, IsFin, fun(#{body := Body} = Stream) -> Stream#{body := <<Body/binary, Data/binary>>} end, State)};
handle_info({gun_error, _Conn, Ref, Reason}, #{streams := Streams} = State) ->
    ?LOG_WARNING(#{msg => apns_stream_error, reason => Reason}),
    {noreply, State#{streams := maps:remove(Ref, Streams)}};
handle_info({gun_down, _Conn, _Protocol, Reason, KilledStreams}, #{streams := Streams} = State) ->
    ?LOG_WARNING(#{msg => apns_connection_down, reason => Reason, lost_notifications => length(KilledStreams)}),
    {noreply, State#{streams := maps:without(KilledStreams, Streams)}};
handle_info({'DOWN', _MonitorRef, process, Connection, Reason}, #{connections := Connections} = State) ->
    ?LOG_WARNING(#{msg => apns_connection_closed, reason => Reason}),
    {noreply, State#{connections := maps:filter(fun(_Env, Pid) -> Pid =/= Connection end, Connections)}};
handle_info(_Info, State) ->
    {noreply, State}.

update_stream(Ref, IsFin, Update, #{streams := Streams} = State) ->
    case Streams of
        #{Ref := Stream} when IsFin =:= fin ->
            finish(Update(Stream), State#{streams := maps:remove(Ref, Streams)});
        #{Ref := Stream} ->
            State#{streams := Streams#{Ref := Update(Stream)}};
        #{} ->
            State
    end.

finish(#{status := 200}, State) ->
    State;
finish(#{status := Status, token := Token, body := Body}, State) ->
    Reason = reason(Body),
    case {Status, Reason} of
        {410, _} ->
            unregister(Token, Reason);
        {400, <<"BadDeviceToken">>} ->
            unregister(Token, Reason);
        {400, <<"DeviceTokenNotForTopic">>} ->
            unregister(Token, Reason);
        {403, _} ->
            ?LOG_ERROR(#{msg => apns_rejected_provider_token, reason => Reason});
        _ ->
            ?LOG_WARNING(#{msg => apns_push_failed, status => Status, reason => Reason})
    end,
    %% A rejected provider token is renewed on the next push.
    case Status of
        403 -> State#{provider_token := undefined};
        _ -> State
    end.

unregister(Token, Reason) ->
    ?LOG_INFO(#{msg => removing_device, reason => Reason}),
    spacepush_registry:unregister(Token).

reason(Body) ->
    try json:decode(Body) of
        #{<<"reason">> := Reason} -> Reason;
        _ -> undefined
    catch
        error:_ -> undefined
    end.

provider_token(#{provider_token := Current, key := Key, key_id := KeyId, team_id := TeamId} = State) ->
    Now = erlang:system_time(second),
    case Current of
        {Token, IssuedAt} when Now - IssuedAt < ?TOKEN_LIFETIME_S ->
            {Token, State};
        _ ->
            Token = spacepush_jwt:token(KeyId, TeamId, Key, Now),
            {Token, State#{provider_token := {Token, Now}}}
    end.

connection(Environment, #{connections := Connections} = State) ->
    case Connections of
        #{Environment := Pid} ->
            {Pid, State};
        #{} ->
            Host = host(Environment),
            {ok, Pid} = gun:open(Host, 443, #{
                protocols => [http2],
                transport => tls,
                tls_opts => spacepush_tls:client_opts(Host)
            }),
            monitor(process, Pid),
            {Pid, State#{connections := Connections#{Environment => Pid}}}
    end.

host(sandbox) -> "api.sandbox.push.apple.com";
host(production) -> "api.push.apple.com".

env(Key) ->
    application:get_env(spacepush, Key, undefined).
