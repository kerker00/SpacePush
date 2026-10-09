-module(spacepush_directory).
-moduledoc """
The list of spaces from the SpaceAPI aggregator, loaded at start and then
every `directory_refresh_ms`.

It is the allowlist: SpacePush only fetches, serves and accepts subscriptions
for endpoints listed here. It also backs `GET /v1/directory`. The list is
saved to disk, so a restart while the aggregator is down keeps the last one;
a failed refresh keeps the current one.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, loaded/0, known/1, entries/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TABLE, spacepush_directory).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "True once a list is available, from the aggregator or from disk.".
-spec loaded() -> boolean().
loaded() ->
    ets:info(?TABLE, size) > 0.

-spec known(binary()) -> boolean().
known(Endpoint) ->
    ets:member(?TABLE, Endpoint).

-spec entries() -> [spacepush_state:directory_entry()].
entries() ->
    [Entry || {_Endpoint, Entry} <- ets:tab2list(?TABLE)].

init([]) ->
    ets:new(?TABLE, [named_table, protected, set, {read_concurrency, true}]),
    File = env(directory_file),
    %% Safe decoding needs the entries' atoms to exist, so load their module first.
    {module, spacepush_state} = code:ensure_loaded(spacepush_state),
    replace(restore(spacepush_store:load(File, none))),
    self() ! refresh,
    {ok, #{file => File}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh, #{file := File} = State) ->
    Url = env(aggregator_url),
    %% The list of all spaces is far larger than a single space's document.
    case spacepush_fetch:get(Url, env(directory_max_bytes)) of
        {ok, Body} ->
            try spacepush_state:parse_directory(Body, erlang:system_time(second), env(max_data_age_s)) of
                [] ->
                    ?LOG_WARNING(#{msg => directory_empty, url => Url});
                Entries ->
                    replace(Entries),
                    spacepush_store:save(File, {directory, 1, Entries})
            catch
                Class:Reason ->
                    ?LOG_WARNING(#{msg => directory_unreadable, url => Url, class => Class, reason => Reason})
            end;
        {error, Reason} ->
            ?LOG_WARNING(#{msg => directory_fetch_failed, url => Url, reason => Reason})
    end,
    erlang:send_after(env(directory_refresh_ms), self(), refresh),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

replace([]) ->
    ok;
replace(Entries) ->
    ets:delete_all_objects(?TABLE),
    ets:insert(?TABLE, [{Endpoint, Entry} || #{endpoint := Endpoint} = Entry <- Entries]),
    ok.

%% Keeps only well-formed entries of a saved list in a known format.
restore({directory, 1, Entries}) when is_list(Entries) ->
    [Entry || #{endpoint := Endpoint} = Entry <- Entries, is_binary(Endpoint)];
restore(_) ->
    [].

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
