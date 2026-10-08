-module(spacepush_apns).
-moduledoc """
Sends due deliveries from the outbox to APNs over HTTP/2 with token-based
authentication.

Keeps one connection per APNs environment, as Apple recommends, and renews
the provider token before it expires after an hour. At most
`apns_max_in_flight` requests are open at a time, each with a deadline.
What happens to a delivery depends on the result (see `classify/2`):

- `delivered` and `drop`: removed from the outbox
- `invalid_token`: removed, together with the registration it was sent for
- `retry` and `renew_token` (429, 5xx, timeout, lost connection, expired
  provider token): retried with exponential backoff until it expires

Without a configured key file, deliveries are only logged.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, wake/0, classify/2, backoff_ms/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% Apple rejects provider tokens older than an hour.
-define(TOKEN_LIFETIME_S, 50 * 60).
-define(TICK_MS, 1000).
-define(BASE_BACKOFF_MS, 5000).
-define(MAX_BACKOFF_MS, 300000).

-type result() :: delivered | invalid_token | renew_token | retry | drop.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Tells the sender that new deliveries are waiting.".
-spec wake() -> ok.
wake() ->
    gen_server:cast(?MODULE, wake).

-doc "Decides what to do with a delivery from the APNs status and error reason.".
-spec classify(pos_integer(), binary() | undefined) -> result().
classify(200, _Reason) -> delivered;
classify(410, _Reason) -> invalid_token;
classify(400, Reason) when Reason =:= <<"BadDeviceToken">>; Reason =:= <<"DeviceTokenNotForTopic">> -> invalid_token;
classify(403, Reason) when Reason =:= <<"ExpiredProviderToken">>; Reason =:= <<"InvalidProviderToken">> -> renew_token;
classify(Status, _Reason) when Status =:= 429; Status >= 500 -> retry;
classify(_Status, _Reason) -> drop.

-spec backoff_ms(non_neg_integer()) -> pos_integer().
backoff_ms(Attempts) ->
    min(?BASE_BACKOFF_MS bsl min(Attempts, 16), ?MAX_BACKOFF_MS).

init([]) ->
    Key =
        case env(apns_key_file) of
            undefined ->
                ?LOG_WARNING("No APNs key configured, deliveries are only logged"),
                undefined;
            File ->
                spacepush_jwt:read_key(File)
        end,
    erlang:send_after(?TICK_MS, self(), tick),
    {ok, #{
        key => Key,
        key_id => env(apns_key_id),
        team_id => env(apns_team_id),
        topic => env(apns_topic),
        max_in_flight => env(apns_max_in_flight),
        request_timeout_ms => env(apns_request_timeout_ms),
        provider_token => undefined,
        connections => #{},
        in_flight => #{}
    }}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(wake, State) ->
    {noreply, dispatch(State)}.

handle_info(tick, State) ->
    erlang:send_after(?TICK_MS, self(), tick),
    {noreply, dispatch(expire_requests(State))};
handle_info({gun_response, _Conn, Ref, IsFin, Status, _Headers}, State) ->
    {noreply, update_request(Ref, IsFin, fun(Request) -> Request#{status := Status} end, State)};
handle_info({gun_data, _Conn, Ref, IsFin, Data}, State) ->
    Append = fun(#{body := Body} = Request) -> Request#{body := <<Body/binary, Data/binary>>} end,
    {noreply, update_request(Ref, IsFin, Append, State)};
handle_info({gun_error, _Conn, Ref, Reason}, State) ->
    {noreply, fail_requests([Ref], Reason, State)};
handle_info({gun_error, _Conn, Reason}, State) ->
    ?LOG_WARNING(#{msg => apns_connection_error, reason => Reason}),
    {noreply, State};
handle_info({gun_down, _Conn, _Protocol, Reason, KilledStreams}, State) ->
    {noreply, fail_requests(KilledStreams, Reason, State)};
handle_info({'DOWN', _MonitorRef, process, Connection, Reason}, #{connections := Connections} = State) ->
    ?LOG_WARNING(#{msg => apns_connection_closed, reason => Reason}),
    Lost = [Ref || Ref := #{connection := Pid} <- maps:get(in_flight, State), Pid =:= Connection],
    Remaining = maps:filter(fun(_Environment, Pid) -> Pid =/= Connection end, Connections),
    {noreply, fail_requests(Lost, Reason, State#{connections := Remaining})};
handle_info(_Info, State) ->
    {noreply, State}.

dispatch(#{in_flight := InFlight, max_in_flight := Max} = State) ->
    case Max - map_size(InFlight) of
        Free when Free > 0 ->
            Ids = [Id || _Ref := #{delivery := #{id := Id}} <- InFlight],
            Due = spacepush_outbox:due(erlang:system_time(millisecond), Ids, Free),
            lists:foldl(fun send/2, State, Due);
        _ ->
            State
    end.

send(#{key := Key, id := Id, token := Token, payload := Payload}, #{key := undefined} = State) ->
    ?LOG_INFO(#{msg => dry_run_delivery, token => Token, payload => Payload}),
    spacepush_outbox:complete(Key, Id),
    State;
send(Delivery, State0) ->
    #{environment := Environment, token := Token, payload := Payload, collapse_id := CollapseId, expires := Expires} =
        Delivery,
    {ProviderToken, State1} = provider_token(State0),
    {Connection, State2} = connection(Environment, State1),
    #{topic := Topic, request_timeout_ms := Timeout, in_flight := InFlight} = State2,
    Headers = [
        {<<"authorization">>, <<"bearer ", ProviderToken/binary>>},
        {<<"apns-topic">>, Topic},
        {<<"apns-push-type">>, <<"alert">>},
        {<<"apns-priority">>, <<"10">>},
        {<<"apns-expiration">>, integer_to_binary(Expires div 1000)},
        {<<"apns-collapse-id">>, CollapseId}
    ],
    Ref = gun:post(Connection, <<"/3/device/", Token/binary>>, Headers, Payload),
    Request = #{
        delivery => Delivery,
        connection => Connection,
        deadline => erlang:system_time(millisecond) + Timeout,
        status => undefined,
        body => <<>>
    },
    State2#{in_flight := InFlight#{Ref => Request}}.

update_request(Ref, IsFin, Update, #{in_flight := InFlight} = State) ->
    case InFlight of
        #{Ref := Request} when IsFin =:= fin ->
            finish(Update(Request), State#{in_flight := maps:remove(Ref, InFlight)});
        #{Ref := Request} ->
            State#{in_flight := InFlight#{Ref := Update(Request)}};
        #{} ->
            State
    end.

finish(#{delivery := Delivery, status := Status, body := Body}, State) ->
    #{key := Key, id := Id, token := Token, environment := Environment, version := Version} = Delivery,
    {Reason, InvalidSince} = error_details(Body),
    case classify(Status, Reason) of
        delivered ->
            spacepush_outbox:complete(Key, Id),
            State;
        invalid_token ->
            ?LOG_INFO(#{msg => device_token_invalid, status => Status, reason => Reason}),
            spacepush_registry:unregister_if(Token, Environment, Version, InvalidSince),
            spacepush_outbox:complete(Key, Id),
            State;
        renew_token ->
            ?LOG_WARNING(#{msg => apns_rejected_provider_token, reason => Reason}),
            schedule_retry(Delivery),
            State#{provider_token := undefined};
        retry ->
            ?LOG_WARNING(#{msg => apns_temporary_failure, status => Status, reason => Reason}),
            schedule_retry(Delivery),
            State;
        drop ->
            ?LOG_ERROR(#{msg => apns_rejected_delivery, status => Status, reason => Reason}),
            spacepush_outbox:complete(Key, Id),
            State
    end.

expire_requests(#{in_flight := InFlight} = State) ->
    Now = erlang:system_time(millisecond),
    Expired = [Ref || Ref := #{deadline := Deadline} <- InFlight, Deadline =< Now],
    [gun:cancel(Connection, Ref) || Ref := #{connection := Connection} <- maps:with(Expired, InFlight)],
    fail_requests(Expired, timeout, State).

fail_requests(Refs, Reason, #{in_flight := InFlight} = State) ->
    Failed = maps:with(Refs, InFlight),
    Failed =/= #{} andalso ?LOG_WARNING(#{msg => apns_requests_failed, count => map_size(Failed), reason => Reason}),
    [schedule_retry(Delivery) || _Ref := #{delivery := Delivery} <- Failed],
    State#{in_flight := maps:without(Refs, InFlight)}.

schedule_retry(#{key := Key, id := Id, attempts := Attempts}) ->
    spacepush_outbox:retry(Key, Id, erlang:system_time(millisecond) + backoff_ms(Attempts)).

error_details(Body) ->
    try json:decode(Body) of
        #{<<"reason">> := Reason} = Error ->
            Timestamp = maps:get(<<"timestamp">>, Error, undefined),
            {Reason, if is_integer(Timestamp) -> Timestamp; true -> undefined end};
        _ ->
            {undefined, undefined}
    catch
        error:_ -> {undefined, undefined}
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
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
