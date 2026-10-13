-module(spacepush_commit).
-moduledoc """
Recoverable hand-off of confirmed tracker changes to the outbox.

A versioned intent is written and synced before any deliveries are published.
The outbox records the commit id durably with its deliveries, so replay also
remains harmless after the sender has completed them. The intended tracker is
then saved before removing the intent. Outbox startup replays pending work
before the sender starts; poller startup restores the intended tracker.
Only one intent may be outstanding; the outbox retains the latest commit id.

This covers process and VM crashes. Parent directories are not synced, so
host power-loss durability is not established by this protocol.
""".

-export([commit/3, recover/1, pending/1, checkpoint/1]).

-type intent() :: {binary(), {tracker, 1, spacepush_state:tracker()}, [spacepush_state:change()]}.

-spec commit(file:filename(), spacepush_state:tracker(), [spacepush_state:change()]) -> ok.
commit(File, Tracker, []) ->
    ensure_finished(File),
    spacepush_store:save(File, spacepush_state:snapshot(Tracker));
commit(File, Tracker, Changes) ->
    ensure_finished(File),
    Id = crypto:strong_rand_bytes(16),
    Snapshot = spacepush_state:snapshot(Tracker),
    true = valid_intent(Id, Snapshot, Changes),
    ok = spacepush_store:save(intent_file(File), {tracker_commit, 1, Id, Snapshot, Changes}),
    checkpoint(intent_saved),
    ok = spacepush_outbox:apply_commit(Id, Changes),
    checkpoint(outbox_saved),
    ok = spacepush_store:save(File, Snapshot),
    checkpoint(tracker_saved),
    ok = clear(File),
    checkpoint(intent_cleared),
    ok.

-spec recover(file:filename()) -> spacepush_state:tracker().
recover(File) ->
    case pending(File) of
        none -> spacepush_state:restore(spacepush_store:load(File, none));
        {Id, Snapshot, Changes} ->
            ok = spacepush_outbox:apply_commit(Id, Changes),
            ok = spacepush_store:save(File, Snapshot),
            ok = clear(File),
            spacepush_state:restore(Snapshot)
    end.

-doc "Reads a pending intent; an unreadable or invalid present file fails closed.".
-spec pending(file:filename()) -> none | intent().
pending(File) ->
    {module, spacepush_state} = code:ensure_loaded(spacepush_state),
    Read = try spacepush_store:read(intent_file(File))
        catch error:{invalid_state_file, _} -> error(invalid_tracker_intent) end,
    case Read of
        missing -> none;
        {ok, Term} ->
            case Term of
                {tracker_commit, 1, Id, Snapshot, Changes} ->
                    case valid_intent(Id, Snapshot, Changes) of
                        true -> {Id, Snapshot, Changes};
                        false -> error(invalid_tracker_intent)
                    end;
                _ -> error(invalid_tracker_intent)
            end
    end.

valid_intent(Id, {tracker, 1, Tracker} = Snapshot, Changes) when
    is_binary(Id), byte_size(Id) =:= 16, is_map(Tracker), is_list(Changes)
->
    spacepush_state:restore(Snapshot) =:= Tracker andalso
        lists:all(fun(Change) -> valid_change(Change, Tracker) end, Changes);
valid_intent(_, _, _) -> false.

valid_change({{Endpoint, Room} = Topic, Name, From, To}, Tracker) when
    is_binary(Endpoint), is_binary(Name)
->
    States = [open, closed, keyholder, member, open_plus, closing],
    spacepush_device:valid_room(Room) andalso From =/= To andalso
        lists:member(From, States) andalso lists:member(To, States) andalso
        case Tracker of #{Topic := #{confirmed := To, candidate := none}} -> true; _ -> false end;
valid_change(_, _) -> false.

ensure_finished(File) ->
    case pending(File) of none -> ok; _ -> error(unfinished_tracker_commit) end.

clear(File) ->
    case file:delete(intent_file(File)) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> error({tracker_intent_delete_failed, Reason})
    end.

intent_file(File) -> File ++ ".intent".

-ifdef(TEST).
checkpoint(Stage) ->
    case application:get_env(spacepush, tracker_commit_hook) of
        {ok, Hook} -> Hook(Stage);
        undefined -> ok
    end.
-else.
checkpoint(_Stage) -> ok.
-endif.
