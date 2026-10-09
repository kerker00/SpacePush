-module(spacepush_registry).
-moduledoc """
Device registrations, kept on disk in DETS and mirrored in ETS.

Each device token maps to its APNs environment, the topics it subscribed to,
a version that grows with every registration and the app's platform
(`<<"ios">>`, `<<"macos">>` or `<<"unknown">>` for apps that do not say). A topic index in ETS
answers `subscribers/1` without going through this process.

The number of registrations is capped, and registrations that were not
renewed within `registration_ttl_days` expire. The apps renew on every launch.

On disk a registration is `{Token, {registration, 2, Environment, Topics, Version, Platform}}`.
Format 1 records, without a platform, and records of the unversioned
prototype format, which stored seconds, are migrated on start.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, register/3, register/4, unregister/1, unregister_if/4, subscribers/1, lookup/1, subscribed_endpoints/0]).
-export([summary/0]).
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

-spec register(binary(), environment(), [spacepush_state:topic()]) -> ok | {error, full}.
register(Token, Environment, Topics) ->
    register(Token, Environment, Topics, <<"unknown">>).

-doc "Adds or replaces a registration. Fails for a new token once the cap is reached.".
-spec register(binary(), environment(), [spacepush_state:topic()], platform()) -> ok | {error, full}.
register(Token, Environment, Topics, Platform) ->
    gen_server:call(?MODULE, {register, Token, Environment, Topics, Platform}).

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

-doc "The current registration of a token, read from ETS.".
-spec lookup(binary()) -> {ok, environment(), [spacepush_state:topic()], version()} | error.
lookup(Token) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, Environment, Topics, Version, _Platform}] -> {ok, Environment, Topics, Version};
        [] -> error
    end.

-doc "Every endpoint at least one device subscribed to, read from ETS.".
-spec subscribed_endpoints() -> [binary()].
subscribed_endpoints() ->
    lists:usort([Endpoint || {{Endpoint, _Room}, _Token} <- ets:tab2list(?INDEX)]).

-spec subscribers(spacepush_state:topic()) -> [subscriber()].
subscribers(Topic) ->
    [
        {Token, Environment, Version}
     || {_Topic, Token} <- ets:lookup(?INDEX, Topic),
        {_Token, Environment, _Topics, Version, _Platform} <- ets:lookup(?REGISTRATIONS, Token)
    ].

-doc """
Registered devices for the statistics: in total, per APNs environment, per
platform, and per subscribed space or room with the space's name.
""".
-spec summary() -> map().
summary() ->
    Registrations = ets:tab2list(?REGISTRATIONS),
    Count = fun(Key) ->
        lists:foldl(fun(Registration, Acc) -> maps:update_with(Key(Registration), fun(N) -> N + 1 end, 1, Acc) end, #{}, Registrations)
    end,
    Names = maps:from_list([{Endpoint, Name} || #{endpoint := Endpoint, name := Name} <- spacepush_directory:entries()]),
    Topics = lists:foldl(
        fun({Topic, _Token}, Acc) -> maps:update_with(Topic, fun(N) -> N + 1 end, 1, Acc) end, #{}, ets:tab2list(?INDEX)
    ),
    Subscriptions = [
        #{<<"endpoint">> => Endpoint, <<"room">> => Room, <<"name">> => maps:get(Endpoint, Names, null), <<"devices">> => N}
     || {Endpoint, Room} := N <- Topics
    ],
    #{
        <<"total">> => length(Registrations),
        <<"cap">> => env(max_registrations),
        <<"by_environment">> => Count(fun({_Token, Environment, _Topics, _Version, _Platform}) -> Environment end),
        <<"by_platform">> => Count(fun({_Token, _Environment, _Topics, _Version, Platform}) -> Platform end),
        <<"subscriptions">> => lists:reverse(lists:sort(fun(#{<<"devices">> := A}, #{<<"devices">> := B}) -> A =< B end, Subscriptions))
    }.

init([]) ->
    process_flag(trap_exit, true),
    File = env(registry_file),
    ok = filelib:ensure_dir(File),
    {ok, ?DETS} = dets:open_file(?DETS, [{file, File}, {type, set}]),
    ets:new(?REGISTRATIONS, [named_table, protected, set, {read_concurrency, true}]),
    ets:new(?INDEX, [named_table, protected, bag, {read_concurrency, true}]),
    load(),
    remove_expired(),
    erlang:send_after(?EXPIRY_CHECK_MS, self(), remove_expired),
    {ok, #{}}.

handle_call({register, Token, Environment, Topics, Platform}, _From, State) ->
    Reply =
        case ets:lookup(?REGISTRATIONS, Token) of
            [{Token, _Environment, _Topics, Previous, _Platform}] ->
                spacepush_stats:count(<<"registrations.renewed">>),
                store({Token, Environment, Topics, next_version(Previous), Platform});
            [] ->
                case ets:info(?REGISTRATIONS, size) < env(max_registrations) of
                    true ->
                        spacepush_stats:count(<<"registrations.new">>),
                        store({Token, Environment, Topics, next_version(0), Platform});
                    false ->
                        spacepush_stats:count(<<"registrations.rejected_full">>),
                        {error, full}
                end
        end,
    {reply, Reply, State};
handle_call({unregister, Token}, _From, State) ->
    ets:member(?REGISTRATIONS, Token) andalso spacepush_stats:count(<<"registrations.removed">>),
    {reply, remove(Token), State};
handle_call({unregister_if, Token, Environment, Version, InvalidSince}, _From, State) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, Environment, _Topics, Version, _Platform}] when InvalidSince =:= undefined; Version =< InvalidSince ->
            spacepush_stats:count(<<"registrations.invalid">>),
            remove(Token);
        _ ->
            ok
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
    lists:foreach(
        fun
            ({Token, {registration, 2, Environment, Topics, Version, Platform}}) ->
                insert_ets({Token, Environment, Topics, Version, Platform});
            ({Token, {registration, 1, Environment, Topics, Version}}) ->
                migrate({Token, Environment, Topics, Version, <<"unknown">>});
            ({Token, Environment, Topics, UpdatedAt}) when is_binary(Token), is_integer(UpdatedAt) ->
                migrate({Token, Environment, Topics, UpdatedAt * 1000, <<"unknown">>});
            (Record) ->
                ?LOG_WARNING(#{msg => registration_dropped, record => Record}),
                ok = dets:delete(?DETS, element(1, Record))
        end,
        Records
    ),
    ok = dets:sync(?DETS).

migrate(Registration) ->
    ?LOG_INFO(#{msg => registration_migrated}),
    ok = write(Registration),
    insert_ets(Registration).

store({Token, _Environment, _Topics, _Version, _Platform} = Registration) ->
    ok = write(Registration),
    ok = dets:sync(?DETS),
    delete_ets(Token),
    insert_ets(Registration),
    ok.

write({Token, Environment, Topics, Version, Platform}) ->
    dets:insert(?DETS, {Token, {registration, 2, Environment, Topics, Version, Platform}}).

remove(Token) ->
    ok = dets:delete(?DETS, Token),
    ok = dets:sync(?DETS),
    delete_ets(Token),
    ok.

insert_ets({Token, _Environment, Topics, _Version, _Platform} = Registration) ->
    ets:insert(?REGISTRATIONS, Registration),
    ets:insert(?INDEX, [{Topic, Token} || Topic <- Topics]).

delete_ets(Token) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, _Environment, Topics, _Version, _Platform}] ->
            [ets:delete_object(?INDEX, {Topic, Token}) || Topic <- Topics],
            ets:delete(?REGISTRATIONS, Token);
        [] ->
            true
    end.

%% Versions are registration times in milliseconds, but always increase per token.
next_version(Previous) ->
    max(erlang:system_time(millisecond), Previous + 1).

remove_expired() ->
    Cutoff = erlang:system_time(millisecond) - env(registration_ttl_days) * 86400000,
    Expired = ets:select(?REGISTRATIONS, [{{'$1', '_', '_', '$2', '_'}, [{'<', '$2', Cutoff}], ['$1']}]),
    lists:foreach(fun remove/1, Expired),
    spacepush_stats:count(<<"registrations.expired">>, length(Expired)),
    Expired =/= [] andalso ?LOG_INFO(#{msg => registrations_expired, count => length(Expired)}).

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
