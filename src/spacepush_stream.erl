-module(spacepush_stream).
-moduledoc """
Volatile fan-out of confirmed state changes to open server-sent event streams.

One server admits streams under shared total and per-client limits and owns
their volatile ETS topic index. The poller
calls `broadcast/1` after `spacepush_commit:commit/3` has returned, so the
durable APNs hand-off is complete before anything is streamed. Nothing is
stored: a client that misses an event reads the current state through the
read API when it reconnects.

`broadcast/1` and `broadcast_test/2` never raise and never block on a
subscriber. Broadcasting reads the index directly without calling the server.
""".
-behaviour(gen_server).

-export([start_link/0, child_spec/0, subscribe/2, broadcast/1, broadcast_test/2, event/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TOPICS, spacepush_stream_topics).

-doc "The volatile stream registry, registered as `spacepush_stream`.".
-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{id => ?MODULE, start => {?MODULE, start_link, []}}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Atomically reserves one stream under both limits and indexes its topics.
The returned monitor follows the exact server generation that admitted it;
the caller must close when it receives DOWN, including during initial writes.
The reservation is released when the calling process exits.
""".
-spec subscribe([spacepush_state:topic()], term()) ->
    {ok, reference()} | {error, too_many_streams | full | unavailable}.
subscribe(Topics, Client) ->
    case whereis(?MODULE) of
        undefined -> {error, unavailable};
        Server ->
            Monitor = erlang:monitor(process, Server),
            try gen_server:call(Server, {subscribe, Topics, Client}) of
                ok -> {ok, Monitor};
                Error -> erlang:demonitor(Monitor, [flush]), Error
            catch exit:_ ->
                erlang:demonitor(Monitor, [flush]),
                {error, unavailable}
            end
    end.

init([]) ->
    ets:new(?TOPICS, [named_table, protected, bag, {read_concurrency, true}]),
    {ok, #{subscriptions => #{}, clients => #{}}}.

handle_call({subscribe, Topics0, Client}, {Pid, _Tag},
        #{subscriptions := Subscriptions, clients := Clients} = State) ->
    Count = maps:get(Client, Clients, 0),
    MaxClient = application:get_env(spacepush, stream_max_per_client, 10),
    MaxTotal = application:get_env(spacepush, stream_max_connections, 10000),
    if
        Count >= MaxClient; is_map_key(Pid, Subscriptions) ->
            {reply, {error, too_many_streams}, State};
        map_size(Subscriptions) >= MaxTotal ->
            {reply, {error, full}, State};
        true ->
            Topics = lists:usort(Topics0),
            Monitor = erlang:monitor(process, Pid),
            ets:insert(?TOPICS, [{Topic, Pid} || Topic <- Topics]),
            Subscription = #{monitor => Monitor, client => Client, topics => Topics},
            {reply, ok, State#{subscriptions := Subscriptions#{Pid => Subscription},
                clients := Clients#{Client => Count + 1}}}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

handle_cast(_Message, State) ->
    {noreply, State}.

handle_info({'DOWN', Monitor, process, Pid, _Reason},
        #{subscriptions := Subscriptions, clients := Clients} = State) ->
    case maps:find(Pid, Subscriptions) of
        {ok, #{monitor := Monitor, client := Client, topics := Topics}} ->
            [ets:delete_object(?TOPICS, {Topic, Pid}) || Topic <- Topics],
            Count = maps:get(Client, Clients),
            Clients1 = case Count of
                1 -> maps:remove(Client, Clients);
                _ -> Clients#{Client := Count - 1}
            end,
            {noreply, State#{subscriptions := maps:remove(Pid, Subscriptions), clients := Clients1}};
        _ -> {noreply, State}
    end;
handle_info(_Message, State) ->
    {noreply, State}.

-doc "Sends each change to the streams of its topic.".
-spec broadcast([spacepush_state:change()]) -> ok.
broadcast(Changes) ->
    At = erlang:system_time(second),
    lists:foreach(
        fun({{Endpoint, Room} = Topic, Name, From, To}) ->
            send(Topic, change, #{
                <<"endpoint">> => Endpoint,
                <<"room">> => Room,
                <<"name">> => title(Topic, Name, To),
                <<"from">> => atom_to_binary(From),
                <<"to">> => atom_to_binary(To),
                <<"at">> => At
            })
        end,
        Changes
    ).

-doc "Sends a test event to the streams of one topic; answers how many received it.".
-spec broadcast_test(spacepush_state:topic(), binary()) -> non_neg_integer().
broadcast_test({Endpoint, Room} = Topic, Name) ->
    send(Topic, test, #{
        <<"endpoint">> => Endpoint,
        <<"room">> => Room,
        <<"name">> => title(Topic, Name, open),
        <<"test">> => true,
        <<"at">> => erlang:system_time(second)
    }).

-doc "One server-sent event: JSON data never contains a raw newline.".
-spec event(change | test, map()) -> binary().
event(Kind, Data) ->
    iolist_to_binary([<<"event: ">>, atom_to_binary(Kind), <<"\ndata: ">>, json:encode(Data), <<"\n\n">>]).

send(Topic, Kind, Data) ->
    try
        Members = [Pid || {_, Pid} <- ets:lookup(?TOPICS, Topic)],
        Event = event(Kind, Data),
        [Pid ! {?MODULE, Event} || Pid <- Members],
        spacepush_stats:count(<<"stream.events">>, length(Members)),
        length(Members)
    catch
        error:badarg -> 0; %% The volatile registry can be absent/restarting.
        Class:Reason ->
            logger:warning("stream broadcast failed: ~p:~p", [Class, Reason]),
            0
    end.

%% The same cleaned title the APNs alert shows, without changing the APNs code.
title(Topic, Name, State) ->
    #{<<"aps">> := #{<<"alert">> := #{<<"title">> := Title}}} = spacepush_notification:payload(Topic, Name, State),
    Title.
