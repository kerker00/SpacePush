-module(spacepush_poller).
-moduledoc """
Fetches the spaces that are needed every `poll_interval_ms`, stores the
responses in the cache, confirms state changes with `spacepush_state:track/4`
and hands them to the outbox.

A space is needed while a device subscribed to it or an app asked for it
within `watch_window_ms`, and only if the directory lists it. Mainframe's
openState is requested every round for its rooms; its SpaceAPI document is
cached for the apps but not tracked, since openState is the finer source.
Polling and read requests share the cache's per-endpoint admission and
failure backoff. Each successful response generation is observed only once,
including responses already fetched by readers before the poll.

The tracker is saved to disk, so a restart neither forgets a pending change
nor misses one that happened while the service was down.
""".
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    File = env(tracker_file),
    %% Safe decoding needs the tracker's atoms to exist, so load their module first.
    {module, spacepush_state} = code:ensure_loaded(spacepush_state),
    Tracker = spacepush_state:restore(spacepush_store:load(File, none)),
    self() ! poll,
    {ok, #{file => File, tracker => Tracker, seen => #{}}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll, #{file := File, tracker := Tracker} = State) ->
    Started = erlang:monotonic_time(millisecond),
    MainframeUrl = unicode:characters_to_binary(env(mainframe_url)),
    Fetched = spacepush_cache:refresh_many([MainframeUrl | needed()], env(fetch_concurrency)),
    {Results, Seen} = observations(Fetched, maps:get(seen, State, #{})),
    record_fetches(MainframeUrl, Results),
    Observations = lists:append([
        Observations
     || Url := {ok, Observations} <- Results, Url =/= spacepush_state:mainframe_endpoint()
    ]),
    {Tracker1, Changes} = spacepush_state:track(Tracker, Observations, erlang:system_time(second), env(debounce_s)),
    %% The outbox is on disk before the tracker records the change as confirmed:
    %% a crash in between confirms the change again after the restart instead
    %% of losing it.
    ok = spacepush_outbox:enqueue(Changes),
    Tracker1 =/= Tracker andalso spacepush_store:save(File, spacepush_state:snapshot(Tracker1)),
    record_changes(Changes),
    spacepush_stats:count(<<"poll.rounds">>),
    spacepush_stats:duration(<<"poll">>, erlang:monotonic_time(millisecond) - Started),
    erlang:send_after(env(poll_interval_ms), self(), poll),
    {noreply, State#{tracker := Tracker1, seen => Seen}};
handle_info(_Info, State) ->
    {noreply, State}.

%% Mainframe's openState is counted apart from the spaces' documents.
record_fetches(MainframeUrl, Results) ->
    {Mainframe, Spaces} = maps:fold(
        fun(Url, Result, {M, S}) when Url =:= MainframeUrl -> {[Result | M], S};
           (_Url, Result, {M, S}) -> {M, [Result | S]}
        end,
        {[], []},
        Results
    ),
    record_fetches(<<"mainframe">>, Mainframe, <<"mainframe">>),
    record_fetches(<<"spaces">>, Spaces, <<"spaces">>).

record_fetches(Prefix, Results, Source) ->
    {Skipped, Attempted} = lists:partition(fun(R) -> lists:member(R, [{error, backoff}, {error, busy}, {error, unchanged}, {error, superseded}, {error, stale}]) end, Results),
    Ok = length([ok || {ok, _Observations} <- Attempted]),
    spacepush_stats:count(<<Prefix/binary, ".fetch_skipped">>, length(Skipped)),
    spacepush_stats:count(<<Prefix/binary, ".fetch_ok">>, Ok),
    spacepush_stats:count(<<Prefix/binary, ".fetch_failed">>, length(Attempted) - Ok),
    Ok > 0 andalso spacepush_stats:success(Source).

record_changes(Changes) ->
    Mainframe = spacepush_state:mainframe_endpoint(),
    spacepush_stats:count(<<"changes.total">>, length(Changes)),
    spacepush_stats:count(<<"changes.mainframe">>, length([C || {{Endpoint, _Room}, _, _, _} = C <- Changes, Endpoint =:= Mainframe])).

%% Subscribed or recently requested, and listed in the directory.
needed() ->
    Since = erlang:system_time(millisecond) - env(watch_window_ms),
    Candidates = lists:usort(spacepush_registry:subscribed_endpoints() ++ spacepush_cache:watched(Since)),
    [Url || Url <- Candidates, spacepush_directory:known(Url)].

%% The cache owns response publication; the poller never rewrites its timestamp.
observations(Fetched, Seen) ->
    Cutoff = erlang:system_time(millisecond) - env(max_data_age_s) * 1000,
    maps:fold(fun
        (Url, {ok, #{fetched_at := FetchedAt}}, {Results, Previous}) when FetchedAt < Cutoff ->
            {Results#{Url => {error, stale}}, Previous};
        (Url, {ok, #{generation := Generation, observations := Observations}}, {Results, Previous}) ->
            case spacepush_cache:lookup(Url) of
                #{generation := Generation} ->
                    case maps:get(Url, Previous, none) of
                        Generation -> {Results#{Url => {error, unchanged}}, Previous};
                        _ -> {Results#{Url => {ok, Observations}}, Previous#{Url => Generation}}
                    end;
                _ -> {Results#{Url => {error, superseded}}, Previous}
            end;
        (Url, {error, _} = Error, {Results, Previous}) ->
            {Results#{Url => Error}, Previous}
    end, {#{}, Seen}, Fetched).

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
