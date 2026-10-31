-module(spacepush_stream).
-moduledoc """
Volatile fan-out of confirmed state changes to open server-sent event streams.

Stream handlers join a `pg` group per topic and one per client. The poller
calls `broadcast/1` after `spacepush_commit:commit/3` has returned, so the
durable APNs hand-off is complete before anything is streamed. Nothing is
stored: a client that misses an event reads the current state through the
read API when it reconnects.

`broadcast/1` and `broadcast_test/2` never raise and never block on a
subscriber, so a missing scope or a slow stream cannot affect the poller.
""".

-export([child_spec/0, subscribe/2, broadcast/1, broadcast_test/2, event/2]).

-define(SCOPE, ?MODULE).

-doc "The `pg` scope, registered as `spacepush_stream`.".
-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{id => ?MODULE, start => {pg, start_link, [?SCOPE]}}.

-doc """
Joins the calling process to each topic's group and to its client's group.
Refuses when the client already holds `stream_max_per_client` streams. The
limit is checked before joining, so concurrent joins can briefly exceed it.
Membership ends when the process exits.
""".
-spec subscribe([spacepush_state:topic()], term()) -> ok | {error, too_many_streams}.
subscribe(Topics, Client) ->
    {ok, Max} = application:get_env(spacepush, stream_max_per_client),
    case length(pg:get_local_members(?SCOPE, {client, Client})) < Max of
        true ->
            ok = pg:join(?SCOPE, {client, Client}, self()),
            lists:foreach(fun(Topic) -> ok = pg:join(?SCOPE, {topic, Topic}, self()) end, Topics);
        false ->
            {error, too_many_streams}
    end.

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
        Members = pg:get_members(?SCOPE, {topic, Topic}),
        Event = event(Kind, Data),
        [Pid ! {?MODULE, Event} || Pid <- Members],
        spacepush_stats:count(<<"stream.events">>, length(Members)),
        length(Members)
    catch
        Class:Reason ->
            logger:warning("stream broadcast failed: ~p:~p", [Class, Reason]),
            0
    end.

%% The same cleaned title the APNs alert shows, without changing the APNs code.
title(Topic, Name, State) ->
    #{<<"aps">> := #{<<"alert">> := #{<<"title">> := Title}}} = spacepush_notification:payload(Topic, Name, State),
    Title.
