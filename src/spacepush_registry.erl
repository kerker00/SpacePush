-module(spacepush_registry).
-moduledoc """
Device registrations, kept on disk in DETS and mirrored in ETS.

Each device token maps to its APNs environment, the topics it subscribed to
and a version that grows with every registration. A topic index in ETS
answers `subscribers/1` without going through this process.

The number of registrations is capped, and registrations that were not
renewed within `registration_ttl_days` expire. The apps renew on every launch.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, register/3, unregister/1, unregister_if/4, subscribers/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DETS, spacepush_registry).
-define(REGISTRATIONS, spacepush_registrations).
-define(INDEX, spacepush_topic_index).
-define(EXPIRY_CHECK_MS, 3600000).

-type environment() :: sandbox | production.
-type version() :: integer().
-type subscriber() :: {Token :: binary(), environment(), version()}.
-export_type([environment/0, version/0, subscriber/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Adds or replaces a registration. Fails for a new token once the cap is reached.".
-spec register(binary(), environment(), [spacepush_state:topic()]) -> ok | {error, full}.
register(Token, Environment, Topics) ->
    gen_server:call(?MODULE, {register, Token, Environment, Topics}).

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

-spec subscribers(spacepush_state:topic()) -> [subscriber()].
subscribers(Topic) ->
    [
        {Token, Environment, Version}
     || {_Topic, Token} <- ets:lookup(?INDEX, Topic),
        {_Token, Environment, _Topics, Version} <- ets:lookup(?REGISTRATIONS, Token)
    ].

init([]) ->
    process_flag(trap_exit, true),
    File = env(registry_file),
    ok = filelib:ensure_dir(File),
    {ok, ?DETS} = dets:open_file(?DETS, [{file, File}, {type, set}]),
    ets:new(?REGISTRATIONS, [named_table, protected, set, {read_concurrency, true}]),
    ets:new(?INDEX, [named_table, protected, bag, {read_concurrency, true}]),
    dets:traverse(?DETS, fun(Registration) ->
        insert_ets(Registration),
        continue
    end),
    remove_expired(),
    erlang:send_after(?EXPIRY_CHECK_MS, self(), remove_expired),
    {ok, #{}}.

handle_call({register, Token, Environment, Topics}, _From, State) ->
    Reply =
        case ets:lookup(?REGISTRATIONS, Token) of
            [{Token, _Environment, _Topics, Previous}] ->
                store({Token, Environment, Topics, next_version(Previous)});
            [] ->
                case ets:info(?REGISTRATIONS, size) < env(max_registrations) of
                    true -> store({Token, Environment, Topics, next_version(0)});
                    false -> {error, full}
                end
        end,
    {reply, Reply, State};
handle_call({unregister, Token}, _From, State) ->
    {reply, remove(Token), State};
handle_call({unregister_if, Token, Environment, Version, InvalidSince}, _From, State) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, Environment, _Topics, Version}] when InvalidSince =:= undefined; Version =< InvalidSince ->
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

store({Token, _Environment, _Topics, _Version} = Registration) ->
    ok = dets:insert(?DETS, Registration),
    delete_ets(Token),
    insert_ets(Registration),
    ok.

remove(Token) ->
    ok = dets:delete(?DETS, Token),
    delete_ets(Token),
    ok.

insert_ets({Token, _Environment, Topics, _Version} = Registration) ->
    ets:insert(?REGISTRATIONS, Registration),
    ets:insert(?INDEX, [{Topic, Token} || Topic <- Topics]).

delete_ets(Token) ->
    case ets:lookup(?REGISTRATIONS, Token) of
        [{Token, _Environment, Topics, _Version}] ->
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
    Expired = ets:select(?REGISTRATIONS, [{{'$1', '_', '_', '$2'}, [{'<', '$2', Cutoff}], ['$1']}]),
    lists:foreach(fun remove/1, Expired),
    Expired =/= [] andalso ?LOG_INFO(#{msg => registrations_expired, count => length(Expired)}).

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
