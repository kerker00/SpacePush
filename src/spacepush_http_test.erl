-module(spacepush_http_test).
-moduledoc "Server-local POST /v1/test?endpoint=...&room=space broadcasts a fixed test alert.".
-behaviour(cowboy_handler).
-export([init/2]).

init(Req0, Opts) ->
    Req = case spacepush_http_stats:local(Req0) of
        false -> spacepush_http:error_reply(404, <<"not_found">>, Req0);
        true ->
            case cowboy_req:method(Req0) of
                <<"POST">> ->
                    case spacepush_ratelimit:allow(write, spacepush_http:client(Req0)) of
                        true -> handle(Req0);
                        false -> spacepush_http:error_reply(429, <<"rate_limited">>, Req0)
                    end;
                _ -> cowboy_req:reply(405, #{<<"allow">> => <<"POST">>}, Req0)
            end
    end,
    {ok, Req, Opts}.

handle(Req) ->
    case topic(Req) of
        error -> spacepush_http:error_reply(400, <<"invalid_subscription">>, Req);
        {Endpoint, _Room} = Topic ->
            case spacepush_directory:loaded() of
                false -> spacepush_http:error_reply(503, <<"directory_unavailable">>, Req);
                true ->
                    case [Name || #{endpoint := Url, name := Name} <- spacepush_directory:entries(), Url =:= Endpoint] of
                        [] -> spacepush_http:error_reply(404, <<"unknown_space">>, Req);
                        [Name | _] -> enqueue(Topic, Name, Req)
                    end
            end
    end.

topic(Req) ->
    try cowboy_req:match_qs([endpoint, {room, [], <<"space">>}], Req) of
        #{endpoint := Endpoint, room := Room} when is_binary(Endpoint), byte_size(Endpoint) =< 4096 ->
            case spacepush_device:valid_room(Room) of
                true -> {Endpoint, Room};
                false -> error
            end;
        _ -> error
    catch _:_ -> error end.

enqueue(Topic, Name0, Req) ->
    Name = case Name0 of N when is_binary(N) -> N; _ -> <<"SpacePush">> end,
    case spacepush_outbox:enqueue_test(Topic, Name) of
        {ok, Count} ->
            spacepush_http:json_reply(202, #{<<"cache-control">> => <<"no-store">>},
                json:encode(#{<<"recipients">> => Count, <<"test">> => true}), Req);
        {error, cooldown} -> spacepush_http:error_reply(429, <<"test_cooldown">>, Req)
    end.
