-module(spacepush_cache).
-moduledoc """
The latest response of every fetched space, and which spaces apps asked for.

The poller keeps needed entries refreshed. `get/1` serves the HTTP API: it answers
from the cache while the entry is younger than `cache_max_age_ms`, and
otherwise fetches the URL once, however many requests wait for it at the same
time. If that fetch fails, the last good response is returned to the caller;
the HTTP handler checks its observation age before serving it. Every `get/1`
also marks the URL as watched, so the poller keeps it fresh for a while.

`refresh/1` is the poller's path through the same workers and per-URL admission.
Attempts start at least `fetch_min_interval_ms` apart. Failures back off from
that interval up to `fetch_backoff_max_ms`; successful refreshes reset it.
Reads may use an old response during backoff. Pollers can consume a recent
successful read result, with a generation identifying that observation;
failed attempts never return an old response to the poller.

Only responses that parse as a space (or as Mainframe's openState) are stored.
At most `on_demand_fetch_limit` such fetches run at a time, with at most
`on_demand_waiter_limit` callers in total. Workers and callers are monitored;
every fetch ends within `fetch_timeout_ms`. Beyond either capacity limit,
`get/1` answers `{error, busy}` instead of retaining more work.
""".
-behaviour(gen_server).

-export([start_link/0, get/1, refresh/1, refresh_many/2, store/3, lookup/1, watched/1, observe/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(CACHE, spacepush_cache).
-define(WATCH, spacepush_watch).

-type entry() :: #{
    body := binary(),
    observations := [spacepush_state:observation()],
    fetched_at := integer(),
    generation := reference()
}.
-export_type([entry/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get(binary()) -> {ok, entry()} | {error, term()}.
get(Url) ->
    Now = erlang:system_time(millisecond),
    ets:insert(?WATCH, {Url, Now}),
    {ok, MaxAge} = application:get_env(spacepush, cache_max_age_ms),
    case lookup(Url) of
        #{fetched_at := FetchedAt} = Entry when FetchedAt =< Now, Now - FetchedAt < MaxAge ->
            {ok, Entry};
        _ ->
            request(Url, read)
    end.

-doc "Gets a recent successful observation through the shared admission and worker pool.".
-spec refresh(binary()) -> {ok, entry()} | {error, term()}.
refresh(Url) ->
    request(Url, refresh).

-spec refresh_many([binary()], pos_integer()) -> #{binary() => {ok, entry()} | {error, term()}}.
refresh_many(Urls, Concurrency) ->
    spacepush_fetch:many(lists:usort(Urls), Concurrency, fun refresh/1).

request(Url, Mode) ->
    {ok, Timeout} = application:get_env(spacepush, fetch_timeout_ms),
    gen_server:call(?MODULE, {fetch, Url, Mode}, Timeout + 5000).

-doc "Stores a response that was fetched and parsed.".
-spec store(binary(), binary(), [spacepush_state:observation()]) -> true.
store(Url, Body, Observations) ->
    ets:insert(?CACHE, {Url, #{body => Body, observations => Observations, fetched_at => erlang:system_time(millisecond), generation => make_ref()}}).

-spec lookup(binary()) -> entry() | none.
lookup(Url) ->
    case ets:lookup(?CACHE, Url) of
        [{Url, Entry}] -> Entry;
        [] -> none
    end.

-doc "URLs an app asked for at or after `Since` (milliseconds).".
-spec watched(integer()) -> [binary()].
watched(Since) ->
    ets:select(?WATCH, [{{'$1', '$2'}, [{'>=', '$2', Since}], ['$1']}]).

init([]) ->
    ets:new(?CACHE, [named_table, public, set, {read_concurrency, true}, {write_concurrency, true}]),
    ets:new(?WATCH, [named_table, public, set, {write_concurrency, true}]),
    {ok, #{waiting => #{}, workers => #{}, callers => #{}, attempts => #{}}}.

handle_call({fetch, Url, Mode}, From, #{waiting := Waiting, workers := Workers, callers := Callers, attempts := Attempts} = State) ->
    {ok, Limit} = application:get_env(spacepush, on_demand_fetch_limit),
    {ok, WaiterLimit} = application:get_env(spacepush, on_demand_waiter_limit),
    Now = erlang:monotonic_time(millisecond),
    Attempt = maps:get(Url, Attempts, #{next => Now, delay => 0}),
    case {map_size(Callers) >= WaiterLimit, maps:find(Url, Waiting), Now < maps:get(next, Attempt)} of
        {true, _, _} ->
            {reply, {error, busy}, State};
        {false, {ok, Job}, _} ->
            {noreply, add_caller(Url, From, Mode, Job, State)};
        {false, error, true} ->
            {reply, deferred(Url, Mode, Attempt), State};
        {false, error, false} when map_size(Waiting) >= Limit ->
            {reply, {error, busy}, State};
        {false, error, false} ->
            {ok, Timeout} = application:get_env(spacepush, fetch_timeout_ms),
            Deadline = Now + Timeout,
            {Pid, Ref} = spawn_monitor(fun() -> exit({fetched, fetch(Url)}) end),
            Timer = erlang:send_after(max(0, Deadline - erlang:monotonic_time(millisecond)), self(), {fetch_timeout, Url, Ref}),
            Job = #{pid => Pid, ref => Ref, timer => Timer, deadline => Deadline, started => Now, callers => #{}},
            Attempt1 = Attempt#{next := Now + env(fetch_min_interval_ms), last_result => pending},
            State1 = State#{workers := Workers#{Ref => Url}, attempts := Attempts#{Url => Attempt1}},
            {noreply, add_caller(Url, From, Mode, Job, State1)}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, Reason}, #{waiting := Waiting, workers := Workers, callers := Callers} = State) ->
    case maps:find(Ref, Workers) of
        {ok, Url} ->
            #{Url := #{deadline := Deadline}} = Waiting,
            Result = case erlang:monotonic_time(millisecond) < Deadline of
                false -> {error, timeout};
                true ->
                    case Reason of
                        {fetched, Fetched} -> Fetched;
                        Other -> {error, {crashed, Other}}
                    end
            end,
            {noreply, finish(Url, Result, State)};
        error ->
            case maps:find(Ref, Callers) of
                {ok, Url} -> {noreply, remove_caller(Url, Ref, State)};
                error -> {noreply, State}
            end
    end;
handle_info({fetch_timeout, Url, Ref}, #{waiting := Waiting} = State) ->
    case maps:find(Url, Waiting) of
        {ok, #{ref := Ref}} -> {noreply, finish(Url, {error, timeout}, State)};
        _ -> {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{waiting := Waiting}) ->
    maps:foreach(fun(_Url, Job) -> stop_job(Job) end, Waiting).

add_caller(Url, {Pid, _Tag} = From, Mode, #{callers := JobCallers} = Job, #{waiting := Waiting, callers := Callers} = State) ->
    Ref = monitor(process, Pid),
    State#{waiting := Waiting#{Url => Job#{callers := JobCallers#{Ref => {From, Mode}}}}, callers := Callers#{Ref => Url}}.

remove_caller(Url, Ref, #{waiting := Waiting, callers := Callers} = State) ->
    #{Url := #{callers := JobCallers} = Job} = Waiting,
    Job1 = Job#{callers := maps:remove(Ref, JobCallers)},
    State1 = State#{waiting := Waiting#{Url := Job1}, callers := maps:remove(Ref, Callers)},
    case map_size(maps:get(callers, Job1)) of
        0 -> discard(Url, State1);
        _ -> State1
    end.

finish(Url, Result, #{waiting := Waiting} = State) ->
    #{Url := #{callers := Callers} = Job} = Waiting,
    FreshResult = case Result of
        {ok, Body, Observations} ->
            store(Url, Body, Observations),
            {ok, lookup(Url)};
        {error, _} -> Result
    end,
    State1 = discard(Url, after_fetch(Url, FreshResult, Job, State)),
    maps:foreach(fun(_Ref, {From, Mode}) -> gen_server:reply(From, reply(Url, Mode, FreshResult)) end, Callers),
    State1.

reply(Url, read, {error, _} = Error) ->
    case lookup(Url) of
        #{} = Stale -> {ok, Stale};
        none -> Error
    end;
reply(_Url, _Mode, Result) ->
    Result.

deferred(Url, refresh, #{last_result := ok}) ->
    case lookup(Url) of
        #{} = Entry -> {ok, Entry};
        none -> {error, backoff}
    end;
deferred(Url, Mode, _Attempt) ->
    reply(Url, Mode, {error, backoff}).

after_fetch(Url, Result, #{started := Started}, #{attempts := Attempts} = State) ->
    Interval = env(fetch_min_interval_ms),
    Attempt = case Result of
        {ok, _} -> #{next => Started + Interval, delay => 0, last_result => ok};
        {error, _} ->
            #{delay := Previous} = maps:get(Url, Attempts),
            Delay = min(max(Interval, Previous * 2), max(Interval, env(fetch_backoff_max_ms))),
            #{next => erlang:monotonic_time(millisecond) + Delay, delay => Delay, last_result => error}
    end,
    State#{attempts := Attempts#{Url := Attempt}}.

discard(Url, #{waiting := Waiting, workers := Workers, callers := Callers} = State) ->
    {#{ref := Ref, callers := JobCallers} = Job, Rest} = maps:take(Url, Waiting),
    stop_job(Job),
    State#{waiting := Rest, workers := maps:remove(Ref, Workers), callers := maps:without(maps:keys(JobCallers), Callers)}.

stop_job(#{pid := Pid, ref := Ref, timer := Timer, callers := Callers}) ->
    erlang:cancel_timer(Timer),
    demonitor(Ref, [flush]),
    exit(Pid, kill),
    maps:foreach(fun(CallerRef, _From) -> demonitor(CallerRef, [flush]) end, Callers).

fetch(Url) ->
    case spacepush_fetch:get(Url) of
        {ok, Body} ->
            case observe(Url, Body) of
                {ok, Observations} ->
                    {ok, Body, Observations};
                error ->
                    {error, invalid_response}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-doc """
Parses a response: Mainframe's openState URL as rooms, anything else as a
SpaceAPI document. `error` for anything unusable.
""".
-spec observe(binary(), binary()) -> {ok, [spacepush_state:observation()]} | error.
observe(Url, Body) ->
    {ok, MainframeUrl} = application:get_env(spacepush, mainframe_url),
    try
        case Url =:= unicode:characters_to_binary(MainframeUrl) of
            true -> {ok, spacepush_state:parse_mainframe(Body)};
            false -> {ok, [spacepush_state:parse_space(Url, Body)]}
        end
    catch
        _:_ -> error
    end.

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
