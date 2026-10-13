-module(spacepush_apns).
-moduledoc """
Sends due deliveries from the outbox to APNs over HTTP/2 with token-based
authentication.

Keeps one connection per APNs environment, as Apple recommends. Each
environment signs with its own key from `apns_keys`, or with the shared
`apns_key_file` when `apns_keys` is not set; an environment without a key
only logs its deliveries. Provider tokens are kept per environment and
renewed before they expire after an hour, but at most every 20 minutes, as
Apple requires. Gun's automatic reconnect/replay is disabled: requests sent
during graceful closing fail and return to the outbox. A new connection sends
only deliveries selected and checked again by this process. At most
`apns_max_in_flight` requests are open at a time, each with a
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

-ifdef(TEST).
-export([connection_options/1]).
-endif.

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
        connection_retry => #{},
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
    Retry = maps:get(connection_retry, State, #{}),
    Retry1 = maps:fold(fun
        (Environment, #{pid := Pid}, Acc) when Pid =:= Connection ->
            Attempts = maps:get(attempts, maps:get(Environment, Acc, #{}), 0),
            Acc#{Environment => #{attempts => Attempts + 1,
                not_before => erlang:system_time(millisecond) + backoff_ms(Attempts)}};
        (_, _, Acc) -> Acc
    end, Retry, Connections),
    Remaining = maps:filter(fun(_Environment, #{pid := Pid}) -> Pid =/= Connection end, Connections),
    {noreply, fail_requests(Lost, Reason, State#{connections := Remaining, connection_retry => Retry1})};
handle_info(_Info, State) ->
    {noreply, State}.

dispatch(#{in_flight := InFlight, max_in_flight := Max} = State) ->
    case Max - map_size(InFlight) of
        Free when Free > 0 ->
            Now = erlang:system_time(millisecond),
            InFlightKeys = [Key || _Ref := #{delivery := #{key := Key}} <- InFlight],
            %% Clean invalid/unsubscribed rows independently of connection readiness.
            Invalid = spacepush_outbox:due(Now, InFlightKeys, Free, fun(D) -> delivery_environment(D) =:= discard end),
            State1 = lists:foldl(fun prepare/2, State, Invalid),
            State2 = connect_queued_environments(Now, State1),
            Order = case maps:get(next_environment, State2, sandbox) of
                sandbox -> [sandbox, production];
                production -> [production, sandbox]
            end,
            Due = maps:from_list([{Environment, eligible_due(Environment, Now, InFlightKeys, Free, State2)} || Environment <- Order]),
            Active = [Environment || Environment <- Order, maps:get(Environment, Due) =/= []],
            Counts = lists:foldl(fun(#{delivery := #{environment := Environment}}, Acc) ->
                maps:update_with(Environment, fun(N) -> N + 1 end, 1, Acc)
            end, #{}, maps:values(InFlight)),
            dispatch_fair(Active, Due, Counts, Free, Max, State2);
        _ ->
            State
    end.

connect_queued_environments(Now, #{keys := Keys, paused := Paused} = State) ->
    case spacepush_outbox:pending() of
        0 -> State;
        _ -> maps:fold(fun(Environment, _Key, Acc) ->
            case maps:get(Environment, Paused, 0) > Now of
                true -> Acc;
                false ->
                    case connection(Environment, Acc) of
                        {up, _Pid, Updated} -> Updated;
                        {down, Updated} -> Updated
                    end
            end
        end, State, Keys)
    end.

eligible_due(Environment, Now, InFlightKeys, Free, #{keys := Keys, paused := Paused, connections := Connections}) ->
    Ready = case Keys of
        #{Environment := _} -> maps:get(Environment, Paused, 0) =< Now andalso
            case Connections of #{Environment := #{up := true}} -> true; _ -> false end;
        _ -> true
    end,
    case Ready of
        true -> spacepush_outbox:due(Now, InFlightKeys, Free, fun(D) -> delivery_environment(D) =:= Environment end);
        false -> []
    end.

dispatch_fair([], _Due, _Counts, _Free, _Max, State) -> State;
dispatch_fair([Environment], Due, _Counts, Free, _Max, State) ->
    dispatch_environment(Environment, lists:sublist(maps:get(Environment, Due), Free), State);
dispatch_fair([First, Second], Due, Counts, Free, Max, State) ->
    FirstBudget = max(0, (Max + 1) div 2 - maps:get(First, Counts, 0)),
    SecondBudget = max(0, Max div 2 - maps:get(Second, Counts, 0)),
    FirstDue = lists:sublist(maps:get(First, Due), min(Free, FirstBudget)),
    State1 = dispatch_environment(First, FirstDue, State),
    Remaining = maps:get(max_in_flight, State1) - map_size(maps:get(in_flight, State1)),
    dispatch_environment(Second, lists:sublist(maps:get(Second, Due), min(Remaining, SecondBudget)), State1).

dispatch_environment(_Environment, [], State) -> State;
dispatch_environment(Environment, Due, State) ->
    Next = case Environment of sandbox -> production; production -> sandbox end,
    (lists:foldl(fun prepare/2, State, Due))#{next_environment => Next}.

delivery_environment(#{key := {Token, Topic}} = Delivery) ->
    case valid_identity(Token, Topic, maps:get(token, Delivery, undefined)) of
        false -> discard;
        true ->
            case spacepush_registry:lookup(Token) of
                {ok, Environment, Topics, _Version} ->
                    case lists:member(Topic, Topics) of true -> Environment; false -> discard end;
                error -> discard
            end
    end;
delivery_environment(_) -> discard.

valid_identity(Token, Topic, StoredToken) ->
    ValidRoom = case Topic of {_Endpoint, Room} -> spacepush_device:valid_room(Room); _ -> false end,
    spacepush_device:valid_token(Token) andalso StoredToken =:= Token andalso ValidRoom.

%% Checks a due delivery against the current registration and sends it once
%% the connection for its environment is up.
prepare(#{key := {Token, Topic} = Key, id := Id} = Delivery, State) ->
    case valid_identity(Token, Topic, maps:get(token, Delivery, undefined)) of
        true -> prepare_registered(Delivery, State);
        false ->
            ?LOG_WARNING(#{msg => delivery_discarded, reason => invalid_device_or_room}),
            spacepush_stats:count(<<"push.invalid_delivery">>),
            case spacepush_device:valid_token(Token) of
                false -> spacepush_registry:unregister(Token);
                true -> ok
            end,
            spacepush_outbox:complete(Key, Id),
            State
    end.

prepare_registered(#{key := {Token, Topic} = Key, id := Id} = Delivery, State) ->
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
            #{payload := Payload} = Delivery,
            ?LOG_INFO(#{msg => dry_run_delivery, environment => Environment, payload => Payload}),
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
    try gun:post(Connection, <<"/3/device/", Token/binary>>, Headers, Payload) of
        Ref ->
            Request = #{
                delivery => Delivery,
                connection => Connection,
                deadline => erlang:system_time(millisecond) + Timeout,
                status => undefined,
                body => <<>>
            },
            State2#{in_flight := InFlight#{Ref => Request}}
    catch
        Class:_Reason ->
            %% Gun can reject a request synchronously. Keep the provider-token
            %% cache, back off this delivery and continue dispatching the rest.
            %% The exception can contain credentials, so log only its class.
            ?LOG_WARNING(#{msg => apns_send_failed, environment => Environment, class => Class}),
            spacepush_stats:count(<<"push.send_failed">>),
            schedule_retry(Delivery),
            State2
    end.

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

%% Reopen through the sender, with backoff; Gun must never replay postponed POSTs.
connection(Environment, #{connections := Connections} = State) ->
    case Connections of
        #{Environment := #{pid := Pid, up := true}} ->
            {up, Pid, State};
        #{Environment := #{up := false}} ->
            {down, State};
        #{} ->
            Retry = maps:get(Environment, maps:get(connection_retry, State, #{}), #{}),
            case maps:get(not_before, Retry, 0) > erlang:system_time(millisecond) of
                true -> {down, State};
                false ->
                    Host = host(Environment),
                    {ok, Pid} = gun:open(Host, 443, connection_options(Host)),
                    monitor(process, Pid),
                    {down, State#{connections := Connections#{Environment => #{pid => Pid, up => false}}}}
            end
    end.

connection_options(Host) ->
    #{protocols => [http2], retry => 0, transport => tls,
        tls_opts => spacepush_tls:client_opts(Host)}.

set_up(Connection, Up, #{connections := Connections} = State) ->
    Updated = State#{
        connections := maps:map(
            fun
                (_Environment, #{pid := Pid} = Info) when Pid =:= Connection -> Info#{up := Up};
                (_Environment, Info) -> Info
            end,
            Connections
        )
    },
    case Up of
        false -> Updated;
        true ->
            Retry = maps:get(connection_retry, State, #{}),
            Ready = [Environment || Environment := #{pid := Pid} <- Connections, Pid =:= Connection],
            Updated#{connection_retry => maps:without(Ready, Retry)}
    end.

host(sandbox) -> "api.sandbox.push.apple.com";
host(production) -> "api.push.apple.com".

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
