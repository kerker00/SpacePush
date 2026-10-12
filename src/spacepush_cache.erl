-module(spacepush_cache).
-moduledoc """
The latest response of every fetched space, and which spaces apps asked for.

The poller fills the cache every round. `get/1` serves the HTTP API: it answers
from the cache while the entry is younger than `cache_max_age_ms`, and
otherwise fetches the URL once, however many requests wait for it at the same
time. If that fetch fails, the last good response is served. Every `get/1`
also marks the URL as watched, so the poller keeps it fresh for a while.

Only responses that parse as a space (or as Mainframe's openState) are stored.
At most `on_demand_fetch_limit` such fetches run at a time, with at most
`on_demand_waiter_limit` callers in total. Workers and callers are monitored;
every fetch ends within `fetch_timeout_ms`. Beyond either capacity limit,
`get/1` answers `{error, busy}` instead of retaining more work.
""".
-behaviour(gen_server).

-export([start_link/0, get/1, store/3, lookup/1, watched/1, observe/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(CACHE, spacepush_cache).
-define(WATCH, spacepush_watch).

-type entry() :: #{
    body := binary(),
    observations := [spacepush_state:observation()],
    fetched_at := integer()
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
        #{fetched_at := FetchedAt} = Entry when Now - FetchedAt < MaxAge ->
            {ok, Entry};
        _ ->
            {ok, Timeout} = application:get_env(spacepush, fetch_timeout_ms),
            gen_server:call(?MODULE, {fetch, Url}, Timeout + 5000)
    end.

-doc "Stores a response that was fetched and parsed.".
-spec store(binary(), binary(), [spacepush_state:observation()]) -> true.
store(Url, Body, Observations) ->
    ets:insert(?CACHE, {Url, #{body => Body, observations => Observations, fetched_at => erlang:system_time(millisecond)}}).

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
    {ok, #{waiting => #{}, workers => #{}, callers => #{}}}.

handle_call({fetch, Url}, From, #{waiting := Waiting, workers := Workers, callers := Callers} = State) ->
    {ok, Limit} = application:get_env(spacepush, on_demand_fetch_limit),
    {ok, WaiterLimit} = application:get_env(spacepush, on_demand_waiter_limit),
    case {map_size(Callers) >= WaiterLimit, maps:find(Url, Waiting)} of
        {true, _} ->
            {reply, {error, busy}, State};
        {false, {ok, Job}} ->
            {noreply, add_caller(Url, From, Job, State)};
        {false, error} when map_size(Waiting) >= Limit ->
            {reply, {error, busy}, State};
        {false, error} ->
            {ok, Timeout} = application:get_env(spacepush, fetch_timeout_ms),
            {Pid, Ref} = spawn_monitor(fun() -> exit({fetched, fetch(Url)}) end),
            Timer = erlang:send_after(Timeout, self(), {fetch_timeout, Url, Ref}),
            Job = #{pid => Pid, ref => Ref, timer => Timer, callers => #{}},
            {noreply, add_caller(Url, From, Job, State#{workers := Workers#{Ref => Url}})}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, Reason}, #{workers := Workers, callers := Callers} = State) ->
    case maps:find(Ref, Workers) of
        {ok, Url} ->
            Result = case Reason of
                {fetched, Fetched} -> Fetched;
                Other -> {error, {crashed, Other}}
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

add_caller(Url, {Pid, _Tag} = From, #{callers := JobCallers} = Job, #{waiting := Waiting, callers := Callers} = State) ->
    Ref = monitor(process, Pid),
    State#{waiting := Waiting#{Url => Job#{callers := JobCallers#{Ref => From}}}, callers := Callers#{Ref => Url}}.

remove_caller(Url, Ref, #{waiting := Waiting, callers := Callers} = State) ->
    #{Url := #{callers := JobCallers} = Job} = Waiting,
    Job1 = Job#{callers := maps:remove(Ref, JobCallers)},
    State1 = State#{waiting := Waiting#{Url := Job1}, callers := maps:remove(Ref, Callers)},
    case map_size(maps:get(callers, Job1)) of
        0 -> discard(Url, State1);
        _ -> State1
    end.

finish(Url, Result, #{waiting := Waiting} = State) ->
    #{Url := #{callers := Callers}} = Waiting,
    Reply = case Result of
        {ok, Body, Observations} ->
            store(Url, Body, Observations),
            {ok, lookup(Url)};
        {error, _} ->
            case lookup(Url) of
                #{} = Stale -> {ok, Stale};
                none -> Result
            end
    end,
    State1 = discard(Url, State),
    maps:foreach(fun(_Ref, From) -> gen_server:reply(From, Reply) end, Callers),
    State1.

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
