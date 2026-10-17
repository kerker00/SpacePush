-module(spacepush_registry).
-moduledoc """
Device registrations, kept on disk in DETS and mirrored in ETS.

Each device token maps to its APNs environment, the topics it subscribed to,
a version that grows with every registration and the app's platform
(`<<"ios">>`, `<<"macos">>` or `<<"unknown">>` for apps that do not say). A topic index in ETS
answers `subscribers/1` without going through this process.

The number of registrations is capped, and registrations that were not
renewed within `registration_ttl_days` expire. The apps renew on every launch.
Unconfirmed registrations have a separate admission cap and additionally expire
`unconfirmed_registration_ttl_days` after admission, regardless of renewals.
An empty topic list unregisters a device; it never occupies capacity.

On disk format 3 also records the admission time and whether APNs accepted a
notification for this registration. Acceptance does not prove device delivery.
Older formats migrate as unconfirmed, retaining their recorded registration time.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, register/3, register/4, unregister/1, unregister_if/4, subscribers/1, lookup/1, subscribed_endpoints/0]).
-export([summary/0, confirm_if/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DETS, spacepush_registry).
-define(REGISTRATIONS, spacepush_registrations).
-define(INDEX, spacepush_topic_index).
-define(EXPIRY_CHECK_MS, 3600000).

-type environment() :: sandbox | production.
-type version() :: integer().
-type platform() :: binary().
-type subscriber() :: {Token :: binary(), environment(), version()}.
-export_type([environment/0, version/0, platform/0, subscriber/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(binary(), environment(), [spacepush_state:topic()]) -> ok | {error, full | invalid_registration}.
register(Token, Environment, Topics) ->
    register(Token, Environment, Topics, <<"unknown">>).

-doc "Adds or replaces a registration, subject to total and unconfirmed admission caps.".
-spec register(binary(), environment(), [spacepush_state:topic()], platform()) -> ok | {error, full | invalid_registration}.
register(Token, Environment, Topics, Platform) ->
    case valid_registration({Token, Environment, Topics, 0, Platform}) of
        true -> gen_server:call(?MODULE, {register, Token, Environment, Topics, Platform});
        false -> {error, invalid_registration}
    end.

-spec unregister(binary()) -> ok.
unregister(Token) ->
    gen_server:call(?MODULE, {unregister, Token}).

-doc """
Removes a registration APNs reported as invalid, but only if it is still the
one the request was sent for and it was not renewed after `InvalidSince`
(milliseconds, from the APNs response). A device that registered again in
the meantime keeps its new registration.
""".
-spec unregister_if(binary(), environment(), version(), integer() | undefined) -> ok.
unregister_if(Token, Environment, Version, InvalidSince) ->
    gen_server:call(?MODULE, {unregister_if, Token, Environment, Version, InvalidSince}).

-doc "Records APNs acceptance only for the current environment and registration version.".
-spec confirm_if(binary(), environment(), version()) -> ok.
confirm_if(Token, Environment, Version) ->
    gen_server:call(?MODULE, {confirm_if, Token, Environment, Version}).

-doc "The current registration of a token, read from ETS.".
-spec lookup(binary()) -> {ok, environment(), [spacepush_state:topic()], version()} | error.
lookup(Token) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, Environment, Topics, Version, _Platform, _Admission} = Registration] ->
            case active(Registration, now_ms()) of
                true -> {ok, Environment, Topics, Version};
                false -> error
            end;
        [] -> error
    end.

-doc "Every endpoint at least one device subscribed to, read from ETS.".
-spec subscribed_endpoints() -> [binary()].
subscribed_endpoints() ->
    lists:usort([Endpoint || {_Token, _Environment, Topics, _Version, _Platform, _Admission} <- active_registrations(),
        {Endpoint, _Room} <- Topics]).

-spec subscribers(spacepush_state:topic()) -> [subscriber()].
subscribers(Topic) ->
    Cutoffs = cutoffs(now_ms()),
    [
        {Token, Environment, Version}
     || {_Topic, Token} <- ets:lookup(?INDEX, Topic),
        {_Token, Environment, Topics, Version, _Platform, _Admission} = Registration <- ets:lookup(?REGISTRATIONS, Token),
        lists:member(Topic, Topics), active_since(Registration, Cutoffs)
    ].

-doc """
Registered devices for the statistics: in total, per APNs environment, per
platform, and per subscribed space or room.
""".
-spec summary() -> map().
summary() ->
    Registrations = active_registrations(),
    Count = fun(Key) ->
        lists:foldl(fun(Registration, Acc) -> maps:update_with(Key(Registration), fun(N) -> N + 1 end, 1, Acc) end, #{}, Registrations)
    end,
    Topics = lists:foldl(
        fun({_Token, _Environment, DeviceTopics, _Version, _Platform, _Admission}, Acc) ->
            lists:foldl(fun(Topic, Counts) -> maps:update_with(Topic, fun(N) -> N + 1 end, 1, Counts) end,
                Acc, lists:usort(DeviceTopics))
        end, #{}, Registrations
    ),
    Subscriptions = [
        #{<<"endpoint">> => Endpoint, <<"room">> => Room, <<"devices">> => N}
     || {Endpoint, Room} := N <- Topics
    ],
    #{
        <<"total">> => length(Registrations),
        <<"cap">> => env(max_registrations),
        <<"unconfirmed_cap">> => unconfirmed_cap(),
        <<"by_environment">> => Count(fun({_Token, Environment, _Topics, _Version, _Platform, _Admission}) -> Environment end),
        <<"by_platform">> => Count(fun({_Token, _Environment, _Topics, _Version, Platform, _Admission}) -> Platform end),
        <<"by_confirmation">> => Count(fun({_Token, _Environment, _Topics, _Version, _Platform, #{confirmed := Confirmed}}) ->
            case Confirmed of true -> <<"accepted_by_apns">>; false -> <<"unconfirmed">> end
        end),
        <<"subscriptions">> => lists:reverse(lists:sort(fun(#{<<"devices">> := A}, #{<<"devices">> := B}) -> A =< B end, Subscriptions))
    }.

init([]) ->
    process_flag(trap_exit, true),
    File = env(registry_file),
    ok = spacepush_persist:open_dets(?DETS, File),
    ets:new(?REGISTRATIONS, [named_table, protected, set, {read_concurrency, true}]),
    ets:new(?INDEX, [named_table, protected, bag, {read_concurrency, true}]),
    load(),
    remove_expired(),
    erlang:send_after(?EXPIRY_CHECK_MS, self(), remove_expired),
    {ok, #{last_version => 0}}.

handle_call({register, Token, _Environment, [], _Platform}, From, State) ->
    handle_call({unregister, Token}, From, State);
handle_call({register, Token, Environment, Topics, Platform}, _From, State) ->
    Now = now_ms(),
    prepare_admission(Token, Environment, Now),
    %% Keep versions distinct even after removal and immediate re-registration.
    %% A registry restart also restarts the sender via rest_for_one, discarding
    %% requests from the previous process lifetime.
    FreshVersion = next_version(maps:get(last_version, State)),
    Version = case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, _, _, PreviousVersion, _, _}] -> max(FreshVersion, PreviousVersion + 1);
        [] -> FreshVersion
    end,
    Reply =
        case ets:lookup(?REGISTRATIONS, Token) of
            [{Token, OldEnvironment, _Topics, _Previous, _Platform, Admission}] ->
                Reset = maps:get(confirmed, Admission) andalso Environment =/= OldEnvironment,
                case not Reset orelse unconfirmed_available() of
                    true ->
                        spacepush_stats:count(<<"registrations.renewed">>),
                        Updated = case Reset of true -> admission(Now); false -> Admission end,
                        store({Token, Environment, Topics, Version, Platform, Updated});
                    false -> reject_full()
                end;
            [] ->
                case ets:info(?REGISTRATIONS, size) < env(max_registrations) andalso unconfirmed_available() of
                    true ->
                        spacepush_stats:count(<<"registrations.new">>),
                        store({Token, Environment, Topics, Version, Platform, admission(Now)});
                    false -> reject_full()
                end
        end,
    UpdatedState = case Reply of ok -> State#{last_version := Version}; _ -> State end,
    {reply, Reply, UpdatedState};
handle_call({unregister, Token}, _From, State) ->
    ets:member(?REGISTRATIONS, Token) andalso spacepush_stats:count(<<"registrations.removed">>),
    {reply, remove(Token), State};
handle_call({unregister_if, Token, Environment, Version, InvalidSince}, _From, State) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, Environment, _Topics, Version, _Platform, _Admission}] when InvalidSince =:= undefined; Version =< InvalidSince ->
            spacepush_stats:count(<<"registrations.invalid">>),
            remove(Token);
        _ ->
            ok
    end,
    {reply, ok, State};
handle_call({confirm_if, Token, Environment, Version}, _From, State) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, Environment, Topics, Version, Platform, #{confirmed := false} = Admission} = Registration] ->
            case active(Registration, now_ms()) of
                true -> ok = store({Token, Environment, Topics, Version, Platform, Admission#{confirmed := true}});
                false ->
                    spacepush_stats:count(<<"registrations.expired">>),
                    ok = remove(Token)
            end;
        _ -> ok
    end,
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(remove_expired, State) ->
    remove_expired(),
    erlang:send_after(?EXPIRY_CHECK_MS, self(), remove_expired),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    dets:close(?DETS).

%% Fills ETS from DETS, migrating or dropping records in other formats.
load() ->
    Records = dets:foldl(fun(Record, Acc) -> [Record | Acc] end, [], ?DETS),
    lists:foreach(fun load_record/1, Records),
    ok = dets:sync(?DETS).

load_record({Token, {registration, 3, Environment, Topics, Version, Platform, Admission}}) ->
    load_registration({Token, Environment, Topics, Version, Platform, Admission}, false);
load_record({Token, {registration, 2, Environment, Topics, Version, Platform}}) ->
    load_registration({Token, Environment, Topics, Version, Platform, admission(Version)}, true);
load_record({Token, {registration, 1, Environment, Topics, Version}}) ->
    load_registration({Token, Environment, Topics, Version, <<"unknown">>, admission(Version)}, true);
load_record({Token, Environment, Topics, UpdatedAt}) when is_integer(UpdatedAt) ->
    load_registration({Token, Environment, Topics, UpdatedAt * 1000, <<"unknown">>, admission(UpdatedAt * 1000)}, true);
load_record(Record) ->
    ?LOG_WARNING(#{msg => registration_dropped, reason => invalid_registration}),
    ok = dets:delete_object(?DETS, Record).

load_registration({Token, _Environment, Topics, _Version, _Platform, _Admission} = Registration, Migrate) ->
    case {valid_registration(Registration), Topics} of
        {true, []} ->
            ?LOG_INFO(#{msg => registration_dropped, reason => empty_subscriptions}),
            ok = dets:delete(?DETS, Token);
        {true, _} when Migrate -> migrate(Registration);
        {true, _} -> insert_ets(Registration);
        {false, _} -> drop_record(Token)
    end.

drop_record(Token) ->
    %% Never put tokens or complete persisted records in logs.
    ?LOG_WARNING(#{msg => registration_dropped, reason => invalid_registration}),
    ok = dets:delete(?DETS, Token).

valid_registration({Token, Environment, Topics, Version, Platform, #{admitted_at := At, confirmed := Confirmed} = Admission}) ->
    is_integer(At) andalso At >= 0 andalso is_integer(Version) andalso At =< Version andalso
        is_boolean(Confirmed) andalso map_size(Admission) =:= 2 andalso
        valid_registration({Token, Environment, Topics, Version, Platform});
valid_registration({Token, Environment, Topics, Version, Platform}) when
    (Environment =:= sandbox orelse Environment =:= production),
    is_list(Topics), length(Topics) =< 50,
    is_integer(Version), Version >= 0, Version =< 16#7fffffffffffffff
->
    spacepush_device:valid_token(Token) andalso
        lists:member(Platform, [<<"ios">>, <<"macos">>, <<"unknown">>]) andalso
        lists:all(fun spacepush_persist:topic/1, Topics);
valid_registration(_) ->
    false.

migrate(Registration) ->
    ?LOG_INFO(#{msg => registration_migrated}),
    ok = write(Registration),
    insert_ets(Registration).

store({Token, _Environment, Topics, _Version, _Platform, _Admission} = Registration) ->
    OldTopics = case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, _, Previous, _, _, _}] -> Previous;
        [] -> []
    end,
    ok = write(Registration),
    ok = dets:sync(?DETS),
    %% Prepare new index entries before the atomic replacement. Readers check
    %% membership against the registration, so unpublished/stale entries are hidden.
    true = ets:insert(?INDEX, [{Topic, Token} || Topic <- Topics]),
    publication_checkpoint(indexed),
    true = ets:insert(?REGISTRATIONS, Registration),
    publication_checkpoint(published),
    [ets:delete_object(?INDEX, {Topic, Token}) || Topic <- OldTopics, not lists:member(Topic, Topics)],
    ok.

-ifdef(TEST).
publication_checkpoint(Stage) ->
    case application:get_env(spacepush, registry_publication_hook) of
        {ok, Hook} -> Hook(Stage);
        undefined -> ok
    end.
-else.
publication_checkpoint(_Stage) -> ok.
-endif.

write({Token, Environment, Topics, Version, Platform, Admission}) ->
    dets:insert(?DETS, {Token, {registration, 3, Environment, Topics, Version, Platform, Admission}}).

admission(Version) -> #{admitted_at => Version, confirmed => false}.

remove(Token) ->
    ok = dets:delete(?DETS, Token),
    ok = dets:sync(?DETS),
    delete_ets(Token),
    ok.

insert_ets({Token, _Environment, Topics, _Version, _Platform, _Admission} = Registration) ->
    ets:insert(?REGISTRATIONS, Registration),
    ets:insert(?INDEX, [{Topic, Token} || Topic <- Topics]).

delete_ets(Token) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, _Environment, Topics, _Version, _Platform, _Admission}] ->
            [ets:delete_object(?INDEX, {Topic, Token}) || Topic <- Topics],
            ets:delete(?REGISTRATIONS, Token);
        [] ->
            true
    end.

%% Versions are registration times in milliseconds, but always increase per token.
next_version(Previous) ->
    max(now_ms(), Previous + 1).

remove_expired() ->
    remove_expired(now_ms()).

remove_expired(Now) ->
    Cutoffs = cutoffs(Now),
    Expired = [Token || {Token, _, _, _, _, _} = R <- ets:tab2list(?REGISTRATIONS), not active_since(R, Cutoffs)],
    case Expired of
        [] -> ok;
        _ ->
            [ok = dets:delete(?DETS, Token) || Token <- Expired],
            ok = dets:sync(?DETS),
            lists:foreach(fun delete_ets/1, Expired)
    end,
    spacepush_stats:count(<<"registrations.expired">>, length(Expired)),
    Expired =/= [] andalso ?LOG_INFO(#{msg => registrations_expired, count => length(Expired)}).

active(Registration, Now) ->
    active_since(Registration, cutoffs(Now)).

cutoffs(Now) ->
    {Now - env(registration_ttl_days) * 86400000, Now - env(unconfirmed_registration_ttl_days) * 86400000}.

active_since({_Token, _Environment, _Topics, Version, _Platform, #{admitted_at := At, confirmed := Confirmed}},
    {RenewalCutoff, AdmissionCutoff}) ->
    Version > RenewalCutoff andalso (Confirmed orelse At > AdmissionCutoff).

active_registrations() ->
    Cutoffs = cutoffs(now_ms()),
    [R || R <- ets:tab2list(?REGISTRATIONS), active_since(R, Cutoffs)].

%% Renewals which need no new provisional slot keep their fast path, including
%% older unconfirmed records already above a lowered admission cap. Their
%% original deadline remains unchanged. New admission first releases expired rows.
prepare_admission(Token, Environment, Now) ->
    NeedsSlot = case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, OldEnvironment, _, _, _, #{confirmed := Confirmed}} = Registration] ->
            not active(Registration, Now) orelse (Confirmed andalso OldEnvironment =/= Environment);
        [] -> true
    end,
    NeedsSlot andalso remove_expired(Now),
    ok.

unconfirmed_cap() ->
    min(env(max_registrations), env(max_unconfirmed_registrations)).

unconfirmed_available() ->
    ets:select_count(?REGISTRATIONS, [{{'_', '_', '_', '_', '_', #{confirmed => false}}, [], [true]}]) < unconfirmed_cap().

reject_full() ->
    spacepush_stats:count(<<"registrations.rejected_full">>),
    {error, full}.

-ifdef(TEST).
now_ms() ->
    case application:get_env(spacepush, registry_clock) of
        {ok, Clock} -> Clock();
        undefined -> erlang:system_time(millisecond)
    end.
-else.
now_ms() -> erlang:system_time(millisecond).
-endif.

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
