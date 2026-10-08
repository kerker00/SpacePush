-module(spacepush_poller).
-moduledoc """
Polls the SpaceAPI aggregator and Mainframe's openState endpoint, confirms
state changes with `spacepush_state:track/4` and hands them to the outbox.

The tracker is saved to disk, so a restart neither forgets a pending change
nor misses one that happened while the service was down.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(USER_AGENT, "SpacePush/0.2 (+https://github.com/kerker00/SpacePush)").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    File = env(tracker_file),
    self() ! poll,
    {ok, #{file => File, tracker => spacepush_store:load(File, #{})}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll, #{file := File, tracker := Tracker} = State) ->
    Now = erlang:system_time(second),
    MaxAge = env(max_data_age_s),
    Observations =
        observe(env(aggregator_url), fun(Body) -> spacepush_state:parse_aggregator(Body, Now, MaxAge) end) ++
            observe(env(mainframe_url), fun spacepush_state:parse_mainframe/1),
    {Tracker1, Changes} = spacepush_state:track(Tracker, Observations, Now, env(debounce_s)),
    %% Enqueue before saving: a crash in between confirms the change again
    %% after the restart instead of losing it.
    ok = spacepush_outbox:enqueue(Changes),
    Tracker1 =/= Tracker andalso spacepush_store:save(File, Tracker1),
    erlang:send_after(env(poll_interval_ms), self(), poll),
    {noreply, State#{tracker := Tracker1}};
handle_info(_Info, State) ->
    {noreply, State}.

%% A failing source is skipped for this round; the other one is still read.
observe(Url, Parse) ->
    case fetch(Url) of
        {ok, Body} ->
            try
                Parse(Body)
            catch
                Class:Reason ->
                    ?LOG_WARNING(#{msg => unreadable_response, url => Url, class => Class, reason => Reason}),
                    []
            end;
        {error, Reason} ->
            ?LOG_WARNING(#{msg => fetch_failed, url => Url, reason => Reason}),
            []
    end.

fetch(Url) ->
    #{host := Host} = uri_string:parse(Url),
    Request = {Url, [{"user-agent", ?USER_AGENT}, {"accept", "application/json"}]},
    HttpOptions = [{timeout, 20000}, {connect_timeout, 10000}, {ssl, spacepush_tls:client_opts(Host)}],
    case httpc:request(get, Request, HttpOptions, [{body_format, binary}]) of
        {ok, {{_Version, 200, _Phrase}, _Headers, Body}} -> {ok, Body};
        {ok, {{_Version, Status, _Phrase}, _Headers, _Body}} -> {error, {http_status, Status}};
        {error, Reason} -> {error, Reason}
    end.

env(Key) ->
    {ok, Value} = application:get_env(spacepush, Key),
    Value.
