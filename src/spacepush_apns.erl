-module(spacepush_apns).
-moduledoc """
Sends due deliveries from the outbox to APNs over HTTP/2 with token-based
authentication.

Keeps one connection per APNs environment, as Apple recommends. Each
environment signs with its own key from `apns_keys`, or with the shared
`apns_key_file` when `apns_keys` is not set; an environment without a key
only logs its deliveries. Provider tokens are kept per environment and
renewed before they expire after an hour, but at most every 20 minutes, as
Apple requires. Requests are only sent on
a connection that is up, so a request that times out was never queued inside
gun. At most `apns_max_in_flight` requests are open at a time, each with a
deadline, and at most one per device and topic.

Right before sending, the delivery is checked against the current
registration: if the device unregistered or dropped the topic, it is
discarded; otherwise it goes to the device's current environment.
What happens to a delivery depends on the result (see `classify/2`):

- `delivered` and `drop`: removed from the outbox
- `invalid_token`: removed, together with the registration it was sent for
- `retry` and `renew_token` (429, 5xx, timeout, lost connection, expired
  provider token): retried with exponential backoff until it expires
- `invalid_provider_token` (wrong key, key ID or team, or a key not valid
  for this environment): logged as a configuration error; the environment
  pauses for 20 minutes and then tries once more with a fresh token

Without any key, deliveries are only logged.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, wake/0, classify/2, backoff_ms/1, key_config/3, token_usable/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% Apple rejects provider tokens older than an hour, and refreshing them more
%% often than every 20 minutes.
-define(TOKEN_LIFETIME_S, 50 * 60).
-define(MIN_TOKEN_AGE_S, 20 * 60).
-define(INVALID_KEY_PAUSE_MS, 20 * 60 * 1000).
-define(TICK_MS, 1000).
-define(BASE_BACKOFF_MS, 5000).
-define(MAX_BACKOFF_MS, 300000).

-type result() :: delivered | invalid_token | renew_token | invalid_provider_token | retry | drop.
-type environment() :: spacepush_registry:environment().

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
classify(403, <<"ExpiredProviderToken">>) -> renew_token;
classify(403, <<"InvalidProviderToken">>) -> invalid_provider_token;
classify(Status, _Reason) when Status =:= 429; Status >= 500 -> retry;
classify(_Status, _Reason) -> drop.

-spec backoff_ms(non_neg_integer()) -> pos_integer().
backoff_ms(Attempts) ->
    min(?BASE_BACKOFF_MS bsl min(Attempts, 16), ?MAX_BACKOFF_MS).

-doc """
The key file and key ID per environment: `apns_keys` if set, otherwise the
shared `apns_key_file` and `apns_key_id` for both.
""".
-spec key_config(#{environment() => {file:filename_all(), binary()}} | undefined, file:filename_all() | undefined, binary() | undefined) ->
    #{environment() => {file:filename_all(), binary()}}.
key_config(Keys, _File, _KeyId) when is_map(Keys) -> maps:with([sandbox, production], Keys);
key_config(undefined, undefined, _KeyId) -> #{};
key_config(undefined, File, KeyId) -> #{sandbox => {File, KeyId}, production => {File, KeyId}}.

-doc """
Whether a provider token issued at `IssuedAt` can still be used at `Now`
(seconds): it must be younger than 50 minutes, and after APNs reported it
expired, it is replaced only once it is at least 20 minutes old.
""".
-spec token_usable(integer(), integer(), boolean()) -> boolean().
token_usable(IssuedAt, Now, RenewRequested) ->
    Age = Now - IssuedAt,
    Age < ?TOKEN_LIFETIME_S andalso not (RenewRequested andalso Age >= ?MIN_TOKEN_AGE_S).

init([]) ->
    Keys = maps:map(
        fun(_Environment, {File, KeyId}) -> #{key => spacepush_jwt:read_key(File), key_id => KeyId} end,
        key_config(env(apns_keys), env(apns_key_file), env(apns_key_id))
    ),
    case lists:sort(maps:keys(Keys)) of
        [] -> ?LOG_WARNING("No APNs key configured, deliveries are only logged");
        [production, sandbox] -> ok;
        [Only] -> ?LOG_WARNING(#{msg => apns_key_missing, configured => Only})
    end,
    erlang:send_after(?TICK_MS, self(), tick),
    {ok, #{
        keys => Keys,
        team_id => env(apns_team_id),
        topic => env(apns_topic),
        max_in_flight => env(apns_max_in_flight),
        request_timeout_ms => env(apns_request_timeout_ms),
        tokens => #{},
        renew => #{},
        paused => #{},
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
handle_info({gun_up, Connection, _Protocol}, State) ->
    {noreply, dispatch(set_up(Connection, true, State))};
handle_info({gun_down, Connection, _Protocol, Reason, KilledStreams}, State) ->
    {noreply, fail_requests(KilledStreams, Reason, set_up(Connection, false, State))};
handle_info({'DOWN', _MonitorRef, process, Connection, Reason}, #{connections := Connections} = State) ->
    ?LOG_WARNING(#{msg => apns_connection_closed, reason => Reason}),
    Lost = [Ref || Ref := #{connection := Pid} <- maps:get(in_flight, State), Pid =:= Connection],
    Remaining = maps:filter(fun(_Environment, #{pid := Pid}) -> Pid =/= Connection end, Connections),
    {noreply, fail_requests(Lost, Reason, State#{connections := Remaining})};
handle_info(_Info, State) ->
    {noreply, State}.

dispatch(#{in_flight := InFlight, max_in_flight := Max} = State) ->
    case Max - map_size(InFlight) of
        Free when Free > 0 ->
            Keys = [Key || _Ref := #{delivery := #{key := Key}} <- InFlight],
            Due = spacepush_outbox:due(erlang:system_time(millisecond), Keys, Free),
            lists:foldl(fun prepare/2, State, Due);
        _ ->
            State
    end.

%% Checks a due delivery against the current registration and sends it once
%% the connection for its environment is up.
prepare(#{key := {Token, Topic} = Key, id := Id} = Delivery, State) ->
    case spacepush_registry:lookup(Token) of
        {ok, Environment, Topics, Version} ->
            case lists:member(Topic, Topics) of
                true -> send_when_up(Delivery#{environment := Environment, version := Version}, State);
                false -> discard(Key, Id, State)
            end;
        error ->
            discard(Key, Id, State)
    end.

discard(Key, Id, State) ->
    ?LOG_INFO(#{msg => delivery_discarded, reason => no_longer_subscribed}),
    spacepush_stats:count(<<"push.discarded">>),
    spacepush_outbox:complete(Key, Id),
    State.

send_when_up(#{key := Key, id := Id, environment := Environment} = Delivery, #{keys := Keys, paused := Paused} = State0) ->
    Now = erlang:system_time(millisecond),
    case {Keys, Paused} of
        {#{Environment := _}, #{Environment := Until}} when Until > Now ->
            %% The key was rejected: wait for the pause instead of asking APNs again.
            spacepush_outbox:retry(Key, Id, Until),
            State0;
        {#{Environment := _}, _} ->
            %% Not up yet: the delivery stays due and is picked up again after gun_up.
            case connection(Environment, State0) of
                {up, Connection, State1} -> send(Delivery, Connection, State1);
                {down, State1} -> State1
            end;
        _ ->
            #{token := Token, payload := Payload} = Delivery,
            ?LOG_INFO(#{msg => dry_run_delivery, environment => Environment, token => Token, payload => Payload}),
            spacepush_stats:count(<<"push.dry_run">>),
            spacepush_outbox:complete(Key, Id),
            State0
    end.

send(Delivery, Connection, State0) ->
    #{token := Token, payload := Payload, collapse_id := CollapseId, expires := Expires, environment := Environment} = Delivery,
    {ProviderToken, State2} = provider_token(Environment, State0),
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
    Result = classify(Status, Reason),
    spacepush_stats:count(<<"push.", (atom_to_binary(Result))/binary>>),
    case Result of
        delivered ->
            spacepush_outbox:complete(Key, Id),
            State;
        invalid_token ->
            ?LOG_INFO(#{msg => device_token_invalid, status => Status, reason => Reason}),
            spacepush_registry:unregister_if(Token, Environment, Version, InvalidSince),
            spacepush_outbox:complete(Key, Id),
            State;
        renew_token ->
            ?LOG_WARNING(#{msg => apns_provider_token_expired, environment => Environment}),
            schedule_retry(Delivery),
            #{renew := Renew} = State,
            State#{renew := Renew#{Environment => true}};
        invalid_provider_token ->
            pause(Environment, Delivery, State);
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
    spacepush_stats:count(<<"push.connection_failed">>, map_size(Failed)),
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

%% A wrong key does not fix itself, so the environment waits 20 minutes, which
%% also keeps the next token within Apple's limit, and logs once per pause.
pause(Environment, #{key := Key, id := Id}, #{paused := Paused, tokens := Tokens} = State) ->
    Now = erlang:system_time(millisecond),
    Until = Now + ?INVALID_KEY_PAUSE_MS,
    case Paused of
        #{Environment := Previous} when Previous > Now ->
            ok;
        _ ->
            #{keys := #{Environment := #{key_id := KeyId}}, team_id := TeamId} = State,
            ?LOG_ERROR(#{
                msg => apns_key_rejected,
                environment => Environment,
                key_id => KeyId,
                team_id => TeamId,
                hint => "check that the key is valid for this environment and belongs to the team"
            })
    end,
    spacepush_outbox:retry(Key, Id, Until),
    State#{paused := Paused#{Environment => Until}, tokens := maps:remove(Environment, Tokens)}.

provider_token(Environment, #{tokens := Tokens, renew := Renew} = State) ->
    Now = erlang:system_time(second),
    RenewRequested = maps:get(Environment, Renew, false),
    case Tokens of
        #{Environment := {Token, IssuedAt}} ->
            case token_usable(IssuedAt, Now, RenewRequested) of
                true -> {Token, State};
                false -> new_token(Environment, Now, State)
            end;
        #{} ->
            new_token(Environment, Now, State)
    end.

new_token(Environment, Now, #{keys := Keys, team_id := TeamId, tokens := Tokens, renew := Renew} = State) ->
    #{Environment := #{key := Key, key_id := KeyId}} = Keys,
    Token = spacepush_jwt:token(KeyId, TeamId, Key, Now),
    {Token, State#{tokens := Tokens#{Environment => {Token, Now}}, renew := maps:remove(Environment, Renew)}}.

%% Opens the connection for an environment on first use; gun reconnects on its own.
connection(Environment, #{connections := Connections} = State) ->
    case Connections of
        #{Environment := #{pid := Pid, up := true}} ->
            {up, Pid, State};
        #{Environment := #{up := false}} ->
            {down, State};
        #{} ->
            Host = host(Environment),
            {ok, Pid} = gun:open(Host, 443, #{
                protocols => [http2],
                transport => tls,
                tls_opts => spacepush_tls:client_opts(Host)
            }),
            monitor(process, Pid),
            {down, State#{connections := Connections#{Environment => #{pid => Pid, up => false}}}}
    end.

set_up(Connection, Up, #{connections := Connections} = State) ->
    State#{
        connections := maps:map(
            fun
                (_Environment, #{pid := Pid} = Info) when Pid =:= Connection -> Info#{up := Up};
                (_Environment, Info) -> Info
            end,
            Connections
        )
    }.

host(sandbox) -> "api.sandbox.push.apple.com";
host(production) -> "api.push.apple.com".

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
