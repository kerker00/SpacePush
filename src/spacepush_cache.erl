-module(spacepush_cache).
-moduledoc """
The latest response of every fetched space, and which spaces apps asked for.

The poller fills the cache every round. `get/1` serves the HTTP API: it answers
from the cache while the entry is younger than `cache_max_age_ms`, and
otherwise fetches the URL once, however many requests wait for it at the same
time. If that fetch fails, the last good response is served. Every `get/1`
also marks the URL as watched, so the poller keeps it fresh for a while.

Only responses that parse as a space (or as Mainframe's openState) are stored.
At most `on_demand_fetch_limit` such fetches run at a time; beyond that,
`get/1` answers `{error, busy}` instead of starting more.
""".
-behaviour(gen_server).

-export([start_link/0, get/1, store/3, lookup/1, watched/1, observe/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

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
    {ok, #{waiting => #{}}}.

handle_call({fetch, Url}, From, #{waiting := Waiting} = State) ->
    {ok, Limit} = application:get_env(spacepush, on_demand_fetch_limit),
    case Waiting of
        #{Url := Callers} ->
            {noreply, State#{waiting := Waiting#{Url := [From | Callers]}}};
        #{} when map_size(Waiting) >= Limit ->
            {reply, {error, busy}, State};
        #{} ->
            Self = self(),
            spawn(fun() -> Self ! {fetched, Url, fetch(Url)} end),
            {noreply, State#{waiting := Waiting#{Url => [From]}}}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({fetched, Url, Result}, #{waiting := Waiting} = State) ->
    {Callers, Rest} = maps:take(Url, Waiting),
    [gen_server:reply(Caller, Result) || Caller <- Callers],
    {noreply, State#{waiting := Rest}};
handle_info(_Info, State) ->
    {noreply, State}.

fetch(Url) ->
    Result =
        case spacepush_fetch:get(Url) of
            {ok, Body} ->
                case observe(Url, Body) of
                    {ok, Observations} ->
                        store(Url, Body, Observations),
                        {ok, lookup(Url)};
                    error ->
                        {error, invalid_response}
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    case {Result, lookup(Url)} of
        {{error, _}, #{} = Stale} -> {ok, Stale};
        _ -> Result
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
