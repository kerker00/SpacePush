-module(spacepush_outbox).
-moduledoc """
Deliveries waiting to be sent, kept in DETS so they survive restarts.

There is at most one delivery per device and topic: a newer state replaces
one that has not been sent yet. While a request for a device and topic is in
flight, its successor waits, so states arrive in order. Every delivery has a
unique id, so a late result for a replaced delivery cannot complete or delay
its successor. Deliveries that are still not sent when they expire are dropped.

Notifications for a topic are spaced at least `notification_cooldown_ms`
apart, so a space that keeps switching cannot flood its subscribers: a change
within that time is scheduled for the end of it, and a still newer change
replaces it. The spacing is kept in memory only; after a restart it starts
fresh.

Every change is synced to disk before the call returns.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, enqueue/1, due/3, complete/2, retry/3]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-define(DETS, spacepush_outbox).
-define(TABLE, spacepush_outbox_entries).

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

-doc "Returns up to `Limit` deliveries due at `Now` (ms) whose keys are not in `InFlight`.".
-spec due(integer(), [key()], non_neg_integer()) -> [delivery()].
due(Now, InFlight, Limit) ->
    gen_server:call(?MODULE, {due, Now, InFlight, Limit}).

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
    ok = filelib:ensure_dir(File),
    {ok, ?DETS} = dets:open_file(?DETS, [{file, File}, {type, set}]),
    ets:new(?TABLE, [named_table, protected, set]),
    ?TABLE = dets:to_ets(?DETS, ?TABLE),
    {ok, #{slots => #{}}}.

handle_call({enqueue, Changes}, _From, #{slots := Slots} = State) ->
    Now = erlang:system_time(millisecond),
    {Count, Slots1} = lists:foldl(
        fun(Change, {CountAcc, SlotsAcc}) ->
            {Added, SlotsAcc1} = enqueue_change(Change, Now, SlotsAcc),
            {CountAcc + Added, SlotsAcc1}
        end,
        {0, Slots},
        Changes
    ),
    sync(),
    Count > 0 andalso spacepush_apns:wake(),
    {reply, ok, State#{slots := Slots1}};
handle_call({due, Now, InFlight, Limit}, _From, State) ->
    {Expired, Ready} = ets:foldl(
        fun({Key, #{not_before := NotBefore, expires := Expires} = Delivery}, {ExpiredAcc, ReadyAcc}) ->
            if
                Expires =< Now -> {[Key | ExpiredAcc], ReadyAcc};
                NotBefore > Now -> {ExpiredAcc, ReadyAcc};
                true ->
                    case lists:member(Key, InFlight) of
                        true -> {ExpiredAcc, ReadyAcc};
                        false -> {ExpiredAcc, [Delivery | ReadyAcc]}
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
    {reply, lists:sublist(Oldest, Limit), State};
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

enqueue_change({Topic, Name, _From, To}, Now, Slots) ->
    {ok, Cooldown} = application:get_env(spacepush, notification_cooldown_ms),
    {ok, Ttl} = application:get_env(spacepush, delivery_ttl_ms),
    Slot =
        case Slots of
            #{Topic := Previous} -> max(Now, Previous + Cooldown);
            #{} -> Now
        end,
    Payload = iolist_to_binary(json:encode(spacepush_notification:payload(Topic, Name, To))),
    CollapseId = spacepush_notification:collapse_id(Topic),
    Subscribers = spacepush_registry:subscribers(Topic),
    lists:foreach(
        fun({Token, Environment, Version}) ->
            Key = {Token, Topic},
            %% A pending, unsent delivery is replaced in its own slot.
            NotBefore =
                case ets:lookup(?TABLE, Key) of
                    [{Key, #{not_before := Pending}}] -> Pending;
                    [] -> Slot
                end,
            save(#{
                key => Key,
                id => {erlang:system_time(microsecond), erlang:unique_integer([positive])},
                token => Token,
                environment => Environment,
                version => Version,
                payload => Payload,
                collapse_id => CollapseId,
                attempts => 0,
                not_before => NotBefore,
                expires => NotBefore + Ttl
            })
        end,
        Subscribers
    ),
    ?LOG_INFO(#{msg => state_change_enqueued, topic => Topic, state => To, deliveries => length(Subscribers)}),
    {length(Subscribers), Slots#{Topic => Slot}}.

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
