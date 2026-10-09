-module(spacepush_poller).
-moduledoc """
Fetches the spaces that are needed every `poll_interval_ms`, stores the
responses in the cache, confirms state changes with `spacepush_state:track/4`
and hands them to the outbox.

A space is needed while a device subscribed to it or an app asked for it
within `watch_window_ms`, and only if the directory lists it. Mainframe's
openState is fetched every round for its rooms; its SpaceAPI document is
cached for the apps but not tracked, since openState is the finer source.
A space that keeps failing is skipped for a growing number of rounds.

The tracker is saved to disk, so a restart neither forgets a pending change
nor misses one that happened while the service was down.
""".
-behaviour(gen_server).

-export([start_link/0, due/3, after_round/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(MAX_SKIP_ROUNDS, 30).

-type backoff() :: #{binary() => {Failures :: pos_integer(), NextRound :: non_neg_integer()}}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "The needed URLs that are not skipped because of earlier failures.".
-spec due([binary()], backoff(), non_neg_integer()) -> [binary()].
due(Needed, Backoff, Round) ->
    [Url || Url <- Needed, Round >= element(2, maps:get(Url, Backoff, {0, 0}))].

-doc "Resets a URL after a success; after the n-th failure in a row, skips it for 2^(n-1) rounds, at most 30.".
-spec after_round(#{binary() => spacepush_fetch:result() | invalid}, backoff(), non_neg_integer()) -> backoff().
after_round(Results, Backoff, Round) ->
    maps:fold(
        fun
            (Url, {ok, _Body}, Acc) ->
                maps:remove(Url, Acc);
            (Url, _Failure, Acc) ->
                Failures = element(1, maps:get(Url, Acc, {0, 0})) + 1,
                Acc#{Url => {Failures, Round + min(1 bsl (Failures - 1), ?MAX_SKIP_ROUNDS)}}
        end,
        Backoff,
        Results
    ).

init([]) ->
    File = env(tracker_file),
    %% Safe decoding needs the tracker's atoms to exist, so load their module first.
    {module, spacepush_state} = code:ensure_loaded(spacepush_state),
    Tracker = spacepush_state:restore(spacepush_store:load(File, none)),
    self() ! poll,
    {ok, #{file => File, tracker => Tracker, backoff => #{}, round => 0}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll, #{file := File, tracker := Tracker, backoff := Backoff, round := Round} = State) ->
    MainframeUrl = unicode:characters_to_binary(env(mainframe_url)),
    Due = [MainframeUrl | due(needed(), Backoff, Round)],
    Fetched = spacepush_fetch:many(Due, env(fetch_concurrency)),
    Results = maps:map(fun(Url, Result) -> store(Url, Result) end, Fetched),
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
    Backoff1 = after_round(maps:map(fun(_Url, Result) -> backoff_result(Result) end, Results), Backoff, Round),
    erlang:send_after(env(poll_interval_ms), self(), poll),
    {noreply, State#{tracker := Tracker1, backoff := Backoff1, round := Round + 1}};
handle_info(_Info, State) ->
    {noreply, State}.

%% Subscribed or recently requested, and listed in the directory.
needed() ->
    Since = erlang:system_time(millisecond) - env(watch_window_ms),
    Candidates = lists:usort(spacepush_registry:subscribed_endpoints() ++ spacepush_cache:watched(Since)),
    [Url || Url <- Candidates, spacepush_directory:known(Url)].

%% Keeps usable responses in the cache and returns their observations.
store(Url, {ok, Body}) ->
    case spacepush_cache:observe(Url, Body) of
        {ok, Observations} ->
            spacepush_cache:store(Url, Body, Observations),
            {ok, Observations};
        error ->
            invalid
    end;
store(_Url, {error, Reason}) ->
    {error, Reason}.

backoff_result({ok, _Observations}) -> {ok, ok};
backoff_result(Failure) -> Failure.

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
