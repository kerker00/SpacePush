-module(spacepush_outbox).
-moduledoc """
Deliveries waiting to be sent, kept in DETS so they survive restarts.

There is at most one delivery per device and topic: a newer state replaces
one that has not been sent yet. While a request for a device and topic is in
flight, its successor waits, so states arrive in order. Every delivery has a
unique id, so a late result for a replaced delivery cannot complete or delay
its successor. Deliveries that are still not sent when they expire are dropped.

New states for each device and topic are spaced at least
`notification_cooldown_ms` from the last dispatch attempt. Pending replacements
coalesce at that boundary without reserving extra intervals. Retries of the
same delivery follow their own backoff; their attempt also sets the boundary
for a subsequent state. The spacing is kept in memory only; after a restart it
starts fresh.

Every change is synced to disk before the call returns.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, enqueue/1, apply_commit/2, begin_attempt/3, due/3, due/4, complete/2, retry/3, pending/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-define(DETS, spacepush_outbox).
-define(TABLE, spacepush_outbox_entries).
-define(COMMIT, '$spacepush_commit').

-type key() :: {Token :: binary(), spacepush_state:topic()}.
-type id() :: {integer(), pos_integer()}.
-type delivery() :: #{
    key := key(),
    id := id(),
    token := binary(),
    environment := spacepush_registry:environment(),
    version := spacepush_registry:version(),
    payload := binary(),
    collapse_id := binary(),
    attempts := non_neg_integer(),
    not_before := integer(),
    expires := integer()
}.
-export_type([key/0, id/0, delivery/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Creates a delivery for every subscriber of each changed topic.".
-spec enqueue([spacepush_state:change()]) -> ok.
enqueue([]) ->
    ok;
enqueue(Changes) ->
    gen_server:call(?MODULE, {enqueue, Changes}).

-doc "Checks the current delivery id and reserves a dispatch attempt under the cooldown.".
-spec begin_attempt(key(), id(), integer()) -> ok | skipped.
begin_attempt(Key, Id, Now) ->
    gen_server:call(?MODULE, {begin_attempt, Key, Id, Now}).

-doc "Idempotently replays the outstanding tracker commit, even after its deliveries were completed.".
-spec apply_commit(binary(), [spacepush_state:change()]) -> ok.
apply_commit(Id, Changes) ->
    gen_server:call(?MODULE, {apply_commit, Id, Changes}, infinity).

-doc "Returns up to `Limit` deliveries due at `Now` (ms) whose keys are not in `InFlight`.".
-spec due(integer(), [key()], non_neg_integer()) -> [delivery()].
due(Now, InFlight, Limit) ->
    due(Now, InFlight, Limit, fun(_Delivery) -> true end).

-doc "Selects eligible deliveries before limiting, preserving order within the selection.".
-spec due(integer(), [key()], non_neg_integer(), fun((delivery()) -> boolean())) -> [delivery()].
due(Now, InFlight, Limit, Eligible) ->
    gen_server:call(?MODULE, {due, Now, InFlight, Limit, Eligible}).

-doc "How many deliveries wait to be sent, read from ETS.".
-spec pending() -> non_neg_integer().
pending() ->
    ets:info(?TABLE, size).

-doc "Removes a delivery that was sent or permanently rejected.".
-spec complete(key(), id()) -> ok.
complete(Key, Id) ->
    gen_server:call(?MODULE, {complete, Key, Id}).

-doc "Postpones a delivery after a temporary failure.".
-spec retry(key(), id(), NotBefore :: integer()) -> ok.
retry(Key, Id, NotBefore) ->
    gen_server:call(?MODULE, {retry, Key, Id, NotBefore}).

init([]) ->
    process_flag(trap_exit, true),
    {ok, File} = application:get_env(spacepush, outbox_file),
    ok = spacepush_persist:open_dets(?DETS, File),
    ets:new(?TABLE, [named_table, protected, set]),
    Applied = case dets:lookup(?DETS, ?COMMIT) of
        [] -> none;
        [{?COMMIT, {outbox_commit, 1, Id}}] when is_binary(Id), byte_size(Id) =:= 16 -> Id;
        _ -> error(invalid_outbox_commit)
    end,
    Records = dets:foldl(fun(Record, Acc) -> [Record | Acc] end, [], ?DETS),
    Dropped = lists:foldl(fun
        ({?COMMIT, _}, Count) -> Count;
        (Record, Count) ->
            case valid_record(Record) of
                true ->
                    {Key, Delivery} = Record,
                    Canonical = canonical(Delivery),
                    true = ets:insert(?TABLE, {Key, Canonical}),
                    Canonical =/= Delivery andalso save(Canonical),
                    Count;
                false -> ok = dets:delete_object(?DETS, Record), Count + 1
            end
    end, 0, Records),
    Dropped > 0 andalso ?LOG_WARNING(#{msg => invalid_deliveries_dropped, count => Dropped}),
    sync(),
    State = #{slots => #{}, applied_commit => Applied},
    {ok, TrackerFile} = application:get_env(spacepush, tracker_file),
    %% Complete a possibly partial batch before the supervisor starts APNs.
    Recovered = case spacepush_commit:pending(TrackerFile) of
        none -> State;
        {CommitId, _Snapshot, Changes} -> commit_changes(CommitId, Changes, State)
    end,
    {ok, Recovered}.

canonical(Delivery) ->
    maps:with([key, id, token, environment, version, payload, collapse_id, attempts, not_before, expires], Delivery).

valid_record({{Token, Topic} = Key, #{key := Key, token := Token, id := {At, Sequence},
    environment := Environment, version := Version, payload := Payload, collapse_id := Collapse,
    attempts := Attempts, not_before := NotBefore, expires := Expires}}) ->
    spacepush_device:valid_token(Token) andalso spacepush_persist:topic(Topic) andalso
        spacepush_persist:counter(At) andalso spacepush_persist:counter(Sequence) andalso Sequence > 0 andalso
        (Environment =:= sandbox orelse Environment =:= production) andalso
        spacepush_persist:counter(Version) andalso spacepush_persist:counter(Attempts) andalso
        spacepush_persist:counter(NotBefore) andalso spacepush_persist:counter(Expires) andalso
        Collapse =:= spacepush_notification:collapse_id(Topic) andalso valid_payload(Payload, Topic);
valid_record(_) -> false.

valid_payload(Payload, {Endpoint, Room}) ->
    case spacepush_persist:text(Payload, 4096) of
        false -> false;
        true ->
            try json:decode(Payload) of
                #{<<"endpoint">> := Endpoint, <<"room">> := Room, <<"state">> := State,
                    <<"aps">> := #{<<"alert">> := Alert}} when is_map(Alert) ->
                    lists:member(State, [<<"open">>, <<"closed">>, <<"keyholder">>, <<"member">>, <<"open_plus">>, <<"closing">>]);
                _ -> false
            catch error:_ -> false end
    end.

handle_call({enqueue, Changes}, _From, State) ->
    {Count, State1} = enqueue_changes(Changes, plain, State),
    sync(),
    Count > 0 andalso spacepush_apns:wake(),
    {reply, ok, State1};
handle_call({apply_commit, Id, Changes}, _From, State) ->
    {reply, ok, commit_changes(Id, Changes, State)};
handle_call({begin_attempt, Key, Id, Now}, _From, #{slots := Slots} = State) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, #{id := Id, not_before := NotBefore, expires := Expires} = Delivery}] when Expires > Now ->
            ReadyAt = max(NotBefore, cooldown_until(Key, Id, Now, Slots)),
            case ReadyAt =< Now of
                true -> {reply, ok, State#{slots := Slots#{Key => {Id, Now}}}};
                false ->
                    save(Delivery#{not_before := ReadyAt}),
                    sync(),
                    {reply, skipped, State}
            end;
        _ -> {reply, skipped, State}
    end;
handle_call({due, Now, InFlight, Limit, Eligible}, _From, State) ->
    {Expired, Ready} = ets:foldl(
        fun({Key, #{not_before := NotBefore, expires := Expires} = Delivery}, {ExpiredAcc, ReadyAcc}) ->
            if
                Expires =< Now -> {[Key | ExpiredAcc], ReadyAcc};
                NotBefore > Now -> {ExpiredAcc, ReadyAcc};
                true ->
                    case not lists:member(Key, InFlight) andalso Eligible(Delivery) of
                        false -> {ExpiredAcc, ReadyAcc};
                        true -> {ExpiredAcc, [Delivery | ReadyAcc]}
                    end
            end
        end,
        {[], []},
        ?TABLE
    ),
    lists:foreach(
        fun(Key) ->
            ?LOG_WARNING(#{msg => delivery_expired, topic => element(2, Key)}),
            delete(Key)
        end,
        Expired
    ),
    Expired =/= [] andalso sync(),
    Oldest = lists:sort(fun(#{not_before := A}, #{not_before := B}) -> A =< B end, Ready),
    {ok, Cooldown} = application:get_env(spacepush, notification_cooldown_ms),
    Cutoff = erlang:system_time(millisecond) - Cooldown,
    Slots1 = maps:filter(fun(_Key, {_Id, At}) -> At > Cutoff end, maps:get(slots, State)),
    {reply, lists:sublist(Oldest, Limit), State#{slots := Slots1}};
handle_call({complete, Key, Id}, _From, State) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, #{id := Id}}] -> delete(Key), sync();
        _ -> ok
    end,
    {reply, ok, State};
handle_call({retry, Key, Id, NotBefore}, _From, State) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, #{id := Id, attempts := Attempts} = Delivery}] ->
            save(Delivery#{attempts := Attempts + 1, not_before := NotBefore}),
            sync();
        _ ->
            ok
    end,
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    dets:close(?DETS).

commit_changes(Id, _Changes, #{applied_commit := Id} = State) -> State;
commit_changes(Id, Changes, State) when is_binary(Id), byte_size(Id) =:= 16 ->
    {Count, State1} = enqueue_changes(Changes, commit, State),
    %% A visible commit marker must never precede durable delivery rows.
    sync(),
    spacepush_commit:checkpoint(deliveries_saved),
    ok = dets:insert(?DETS, {?COMMIT, {outbox_commit, 1, Id}}),
    sync(),
    Count > 0 andalso spacepush_apns:wake(),
    State1#{applied_commit := Id}.

enqueue_changes(Changes, Mode, #{slots := Slots} = State) ->
    Now = erlang:system_time(millisecond),
    {Count, Slots1} = lists:foldl(
        fun(Change, {CountAcc, SlotsAcc}) ->
            {Added, SlotsAcc1} = enqueue_change(Change, Now, SlotsAcc, Mode),
            {CountAcc + Added, SlotsAcc1}
        end,
        {0, Slots},
        Changes
    ),
    {Count, State#{slots := Slots1}}.

enqueue_change({Topic, Name, _From, To}, Now, Slots, Mode) ->
    {ok, Ttl} = application:get_env(spacepush, delivery_ttl_ms),
    Payload = iolist_to_binary(json:encode(spacepush_notification:payload(Topic, Name, To))),
    CollapseId = spacepush_notification:collapse_id(Topic),
    Subscribers = spacepush_registry:subscribers(Topic),
    lists:foreach(
        fun({Token, Environment, Version}) ->
            Key = {Token, Topic},
            Id = {erlang:system_time(microsecond), erlang:unique_integer([positive])},
            Slot = cooldown_until(Key, Id, Now, Slots),
            %% Keep a live delivery's slot, but do not inherit an expired row.
            NotBefore =
                case ets:lookup(?TABLE, Key) of
                    [{Key, #{not_before := Pending, expires := Expires}}] when Expires > Now -> cooldown_until(Key, Id, Pending, Slots);
                    _ -> Slot
                end,
            save(#{
                key => Key,
                id => Id,
                token => Token,
                environment => Environment,
                version => Version,
                payload => Payload,
                collapse_id => CollapseId,
                attempts => 0,
                not_before => NotBefore,
                expires => max(Now, NotBefore) + Ttl
            }),
            case Mode of commit -> spacepush_commit:checkpoint(delivery_written); plain -> ok end
        end,
        Subscribers
    ),
    ?LOG_INFO(#{msg => state_change_enqueued, topic => Topic, state => To, deliveries => length(Subscribers)}),
    {length(Subscribers), Slots}.

cooldown_until(Key, Id, Now, Slots) ->
    {ok, Cooldown} = application:get_env(spacepush, notification_cooldown_ms),
    case Slots of
        #{Key := {Id, _At}} -> Now;
        #{Key := {_PreviousId, At}} -> max(Now, At + Cooldown);
        _ -> Now
    end.

save(#{key := Key} = Delivery) ->
    ok = dets:insert(?DETS, {Key, Delivery}),
    true = ets:insert(?TABLE, {Key, Delivery}),
    ok.

delete(Key) ->
    ok = dets:delete(?DETS, Key),
    true = ets:delete(?TABLE, Key),
    ok.

%% DETS keeps writes in a buffer of its own; only sync puts them on disk.
sync() ->
    ok = dets:sync(?DETS).
