-module(spacepush_durability_tests).
-moduledoc "Ends a separate VM abruptly and checks what reached the disk.".
-include_lib("eunit/include/eunit.hrl").

outbox_survives_abrupt_halt_test_() ->
    {timeout, 60, fun outbox_survives_abrupt_halt/0}.

outbox_survives_abrupt_halt() ->
    Dir = filename:absname(spacepush_test_util:tmp_dir("durability")),
    CodePaths = lists:append([["-pa", Path] || Path <- code:get_path(), string:find(Path, "_build") =/= nomatch]),
    Peer =
        case peer:start(#{connection => standard_io, args => CodePaths}) of
            {ok, Pid} -> Pid;
            {ok, Pid, _Node} -> Pid
        end,
    ok = peer:call(Peer, spacepush_test_util, enqueue_in_fresh_node, [Dir]),
    Monitor = monitor(process, Peer),
    peer:cast(Peer, erlang, halt, [0, [{flush, false}]]),
    receive
        {'DOWN', Monitor, process, Peer, _Reason} -> ok
    after 10000 -> error(peer_still_running)
    end,
    {ok, Table} = dets:open_file(outbox_check, [{file, filename:join(Dir, "outbox.dets")}, {repair, true}]),
    Size = dets:info(Table, size),
    ok = dets:close(Table),
    ?assertEqual(1, Size).
