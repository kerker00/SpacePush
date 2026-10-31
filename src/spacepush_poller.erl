-module(spacepush_poller).
-moduledoc """
Refreshes subscribed spaces every `poll_interval_ms`, all other directory
spaces every `background_poll_interval_ms`, through the shared cache workers.
Background deadlines are spread across their interval. Every scheduler tick
selects at most `fetch_concurrency` endpoints, prioritizing subscribers.
Mainframe's openState follows the subscription interval when Mainframe has
subscribers and the background interval otherwise. Its SpaceAPI document is
also refreshed for the directory but not used to track room changes.

App reads keep their on-demand cache path; they do not promote an unsubscribed
space to frequent polling. Fresh read generations for subscribed spaces can be
observed at a scheduler tick without waiting for their next scheduled fetch.
Polling and reads retain their shared admission, failure backoff and freshness
checks. Failed or repeated cached observations never confirm a change.

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
    Tracker = spacepush_commit:recover(File),
    self() ! poll,
    {ok, #{file => File, tracker => Tracker, seen => #{}}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll, #{file := File, tracker := Tracker} = State) ->
    Started = erlang:monotonic_time(millisecond),
    MainframeUrl = unicode:characters_to_binary(env(mainframe_url)),
    Known = [Url || #{endpoint := Url} <- spacepush_directory:entries()],
    Subscribed = [Url || Url <- spacepush_registry:subscribed_endpoints(), spacepush_directory:known(Url)],
    Active = case lists:member(spacepush_state:mainframe_endpoint(), Subscribed) of
        true -> [MainframeUrl | Subscribed];
        false -> Subscribed
    end,
    {Due, Schedule} = spacepush_schedule:select([MainframeUrl | Known], Active,
        maps:get(schedule, State, #{}), Started, env(poll_interval_ms),
        env(background_poll_interval_ms), env(fetch_concurrency)),
    Fetched = spacepush_cache:refresh_many(Due, env(fetch_concurrency)),
    Cached = maps:from_list([{Url, {ok, Entry}} || Url <- Active,
        Entry <- [spacepush_cache:lookup(Url)], Entry =/= none]),
    {Results, Seen} = observations(maps:merge(Cached, Fetched), maps:get(seen, State, #{})),
    %% Count fetch activity only; inspecting a read generation is not a fetch.
    record_fetches(MainframeUrl, maps:with(Due, Results)),
    Observations = lists:append([
        Observations
     || Url := {ok, Observations} <- Results, Url =/= spacepush_state:mainframe_endpoint()
    ]),
    {Tracker1, Changes} = spacepush_state:track(Tracker, Observations, erlang:system_time(second), env(debounce_s)),
    %% A durable intent and idempotent outbox commit recover both sides together.
    Tracker1 =/= Tracker andalso spacepush_commit:commit(File, Tracker1, Changes),
    record_changes(Changes),
    %% Streams are volatile and fed only after the durable commit returned.
    Changes =/= [] andalso spacepush_stream:broadcast(Changes),
    spacepush_stats:count(<<"poll.rounds">>),
    spacepush_stats:duration(<<"poll">>, erlang:monotonic_time(millisecond) - Started),
    erlang:send_after(env(poll_tick_ms), self(), poll),
    {noreply, State#{tracker := Tracker1, seen => maps:with([MainframeUrl | Known], Seen), schedule => Schedule}};
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
