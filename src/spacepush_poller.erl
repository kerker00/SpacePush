-module(spacepush_poller).
-moduledoc """
Polls the SpaceAPI aggregator and Mainframe's openState endpoint and reports
state changes to the dispatcher.

The known states live only in memory. After a restart the first poll just
records the current states, so a restart never sends notifications.
""".
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(USER_AGENT, "SpacePush/0.2 (+https://github.com/kerker00/SpacePush)").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    self() ! poll,
    {ok, #{known => #{}}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll, #{known := Known} = State) ->
    Observations =
        observe(env(aggregator_url), fun spacepush_state:parse_aggregator/1) ++
            observe(env(mainframe_url), fun spacepush_state:parse_mainframe/1),
    {Known1, Changes} = spacepush_state:changes(Known, Observations),
    lists:foreach(fun spacepush_dispatcher:state_changed/1, Changes),
    erlang:send_after(env(poll_interval_ms), self(), poll),
    {noreply, State#{known := Known1}};
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
