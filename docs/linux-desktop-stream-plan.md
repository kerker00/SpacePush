# Plan: Linux desktop notifications via a server-sent event stream

Status: draft for review · branch `feat/linux-desktop-stream` · diagram: [architecture.drawio](architecture.drawio) (pages *Architecture* and *State change flow*)

## Goal

A Linux desktop client (first target: Hyprland; built on SpaceStateKit and SpaceStateCLI) receives the same confirmed
open/close changes the iOS and macOS apps receive through APNs, without a
third-party push service.

A desktop session can keep its own connection open, so SpacePush streams
changes directly over HTTPS as server-sent events (SSE). The client shows them
through the desktop's notification daemon (`org.freedesktop.Notifications`:
mako, swaync, dunst).

## Principles

1. **The commit protocol stays untouched.** `spacepush_commit`, the outbox,
   their file formats and crash tests do not change. The stream is fed *after*
   `spacepush_commit:commit/3` returns.
2. **The stream is volatile.** Nothing is written to disk and there are no
   registrations or tokens. Space states are public data, so the stream needs no
   authentication.
3. **The client resynchronises itself.** On every (re)connect it reads the
   current state through the existing read API (`/v1/spaces`,
   `/v1/mainframe/rooms`), which already applies `max_data_age_s`. A missed
   event only delays the update and never leaves a wrong state.
4. **The streams cannot exhaust the existing API.** Long-lived connections get
   their own listener and connection limit, so iOS registrations and reads keep
   their 1024 connections.

## Protocol (proposal)

    GET /v1/stream?sub=<endpoint>&sub=<endpoint>|<room>
    Accept: text/event-stream

- `sub` is repeated, at most `stream_max_topics` times (default 50). `room`
  defaults to `space`. Each endpoint must be in the directory, as for
  registrations.
- Errors are answered before the stream starts, as JSON like the other handlers:
  - `400` for an invalid or empty subscription,
  - `404` for an unknown endpoint,
  - `429` when the client is rate-limited,
  - `503` while the directory is not loaded or at the connection limit.
- Response headers: `Content-Type: text/event-stream`,
  `Cache-Control: no-store`, `X-Accel-Buffering: no` and
  `X-Content-Type-Options: nosniff`.
- Events:

  ```
  retry: 10000

  event: change
  data: {"endpoint":"…","room":"space","name":"…","from":"closed","to":"open","at":1761926400}

  : ping
  ```

  - `name` is cleaned the same way as the APNs title (`spacepush_notification`).
  - The ping comment is sent every `stream_heartbeat_ms` (default 25 s).
  - `event: test` is used for `/v1/test` broadcasts (D4).
- No `id:` and no `Last-Event-ID` replay. Resync goes through the read API
  (principle 3).

## Server changes

| Step | Change | Files |
| --- | --- | --- |
| S1 | `spacepush_stream`: owns a `pg` scope; `subscribe(Topics)` joins the calling process, `broadcast([change()])` sends `{spacepush_change, Change, At}` to each topic's members; never blocks and never fails the caller | `src/spacepush_stream.erl` (new) |
| S2 | Supervisor child for the scope (placement: D3); add to `registered` | `src/spacepush_sup.erl`, `src/spacepush.app.src` |
| S3 | Poller: call `spacepush_stream:broadcast(Changes)` after `commit/3`, only when `Changes =/= []` | `src/spacepush_poller.erl:66` |
| S4 | SSE handler as a `cowboy_loop`: validate the subscriptions, rate-limit (`read` bucket), `cowboy_req:stream_reply/3`, join groups, heartbeat timer, idle limit (`stream_max_lifetime_ms`, e.g. 1 h, so clients reconnect regularly and pick up server restarts), monitor the pg scope and end the stream if it restarts | `src/spacepush_http_stream.erl` (new) |
| S5 | Second cowboy listener `spacepush_stream_http` on `stream_http_port`, with its own `stream_max_connections`; stop it in `prep_stop/1` | `src/spacepush_app.erl` |
| S6 | Limit concurrent streams per client (e.g. 10 per address) through a small ETS counter in `spacepush_stream`, released by monitor | `src/spacepush_stream.erl` |
| S7 | Statistics: `stream.connects`, `stream.rejected`, `stream.events`; gauge `stream.open` in the report; dashboard tile optional | `src/spacepush_stats.erl`, `priv/stats/*` |
| S8 | Config defaults: `stream_http_ip`, `stream_http_port`, `stream_max_connections`, `stream_max_per_client`, `stream_max_topics`, `stream_heartbeat_ms`, `stream_max_lifetime_ms` | `src/spacepush.app.src`, `config/sys.config`, `deploy/uberspace/sys.config.example` |

### Tests (EUnit, same style as the existing suites)

- `spacepush_stream_tests`:
  - broadcast reaches only the matching topic,
  - a dead subscriber is ignored,
  - broadcasting with no members is a no-op,
  - the per-client limit is released when a stream ends.
- `spacepush_http_stream_tests`, against a real loopback listener with `gun`:
  - status codes for invalid, unknown and too many subscriptions,
  - stream headers,
  - a change event arrives in SSE format,
  - the heartbeat arrives,
  - the lifetime limit closes the stream,
  - a scope restart closes the stream.
- Poller regression: changes are broadcast *after* the commit. Use the existing
  checkpoint mechanism: a crash at any checkpoint must not have broadcast
  anything that recovery would not also commit.
- The existing durability and commit tests must pass unchanged. They are the
  proof that principle 1 holds.

## Operations (Uberspace)

- O1: Path backend `uberspace web backend set push.grafixmafia.net/v1/stream --http --port <stream port>`.
- O2: Before going live, measure the proxy's idle timeout and response
  buffering for SSE. Keep a stream open for over 10 minutes through
  `https://push.grafixmafia.net/v1/stream` and check that the heartbeat arrives
  unbuffered.
- O3: Check the account's open-file limit (`ulimit -n`) against
  `stream_max_connections` plus the API limit.
- O4: README sections: API (`/v1/stream`), Configuration, Deploy step 7,
  Security (public, unauthenticated, limits) and a CHANGELOG entry.

## Linux client (Swift: SpaceStateKit + SpaceStateCLI)

There is no new repository. The client builds on the existing Swift packages,
whose CI already runs on Ubuntu 24.04 with Swift 6.2.

### SpaceStateKit (platform-neutral)

- K0: Spike first. Check that `URLSession` from swift-corelibs-foundation
  (libcurl) delivers small SSE chunks through `didReceive data` without
  buffering, against a local test server that sends one event every few
  seconds. If it does not, the fallback is `AsyncHTTPClient` in the Linux
  target only.
- K1: A pure SSE parser that turns bytes into events (`event`, `data`,
  `retry`, comments) and handles lines split across chunks. Fully unit-tested.
- K2: `SpacePushClient.changes(for: [PushSubscription]) -> AsyncThrowingStream<StateChange, Error>`.
  It uses a delegate-based transfer like `BoundedHTTPTransfer`, without a
  resource timeout. Instead it has an idle timeout of about twice the
  heartbeat, a limit per event, and cancellation.
- K3: A reconnect loop:
  1. read the current state through `spaceObservation(at:)` and
     `mainframeRoomsObservation()`,
  2. open the stream,
  3. on error, wait with exponential backoff plus jitter (cap about 5 min)
     and start again.

  Each step is injectable, as in the CLI's `WatchLoop`, for deterministic tests.
- K4: Release a new Kit version. The Apple apps can adopt `changes(for:)` for
  live updates while they run; that is optional and not part of this plan.

### SpaceStateCLI (Linux desktop)

- L1: `spacestate watch --push`, which uses K3 instead of interval polling.
  The output formats (`text`, `--json`, `--waybar`) and `WatchState` stay as
  they are. Expiry wakes keep working, and so does the existing Waybar module,
  which now updates live.
- L2: `spacestate notify [<space> ...]`, a daemon mode that shows notifications
  through `notify-send --app-name SpaceState --replace-id <id>`. It keeps one id
  per topic, so a newer state replaces the older one, and needs libnotify ≥ 0.8.
  Texts are localised in the CLI, mirroring the apps' `PUSH_STATE_*` keys. A
  direct `sd-bus` C shim is a later option.
- L3: Reconnect immediately on resume. Listening to logind `PrepareForSleep`
  needs D-Bus; until then, a wall-clock jump check in the loop is enough.
- L4: A systemd `--user` unit (`spacestate-notify.service`) plus install docs.
  Spaces come from the CLI's saved spaces.
- L5: Distribution. A static Linux binary via the Static Linux SDK
  (`x86_64-swift-linux-musl`), which removes the Swift runtime dependency.
  Then AUR packages `spacestate` / `spacestate-git`.

## Order of work and commits on this branch

1. Docs: diagram, this plan, README link (this commit).
2. S1 + S2 + tests.
3. S3 + poller regression test.
4. S4 + S5 + S6 + handler tests.
5. S7 + S8.
6. O4 documentation.
7. PR to `main` (CI must be green), then O1 to O3 on the host.
8. Client work in SpaceStateKit (K0 can start in parallel with server step 2), then SpaceStateCLI against the deployed server.

## Open decisions

| # | Question | Proposal |
| --- | --- | --- |
| D1 | Client language | **Decided:** Swift, in SpaceStateKit and SpaceStateCLI |
| D2 | Subscription encoding in the URL | `sub=<endpoint>` and `sub=<endpoint>\|<room>` as above |
| D3 | Supervisor placement of the pg scope | Before `spacepush_poller`. With `rest_for_one`, a scope crash then restarts the poller, sender-side order stays as it is |
| D4 | Should `/v1/test` also reach streams? | Yes, as `event: test`, so the desktop path can be checked end to end |
| D5 | Apply the 5-min notification cooldown to streams? | No: changes are already debounced for 120 s, and the client replaces the notification per topic |
| D6 | Separate port vs. the same listener | Separate port (principle 4), if O1 path backends work as expected |
