# SpacePush

Sends push notifications to the [SpaceState](https://github.com/kerker00/SpaceState) apps when a hackerspace opens or closes.

An Erlang/OTP application that:

- polls the [SpaceAPI](https://spaceapi.io) aggregator (`api.spaceapi.io`) for the state of every listed space; data the aggregator could not refresh recently counts as unknown,
- also polls Mainframe Oldenburg's `openState` endpoint for its rooms (Radstelle, 3D Lab, Machining) and finer states (members only, closing, …),
- confirms a new state only when fresh data still shows it two minutes later, so short flaps send nothing,
- queues one delivery per device and topic in a persistent outbox, where a newer state replaces an unsent older one,
- sends them over APNs HTTP/2 with token-based authentication, retrying temporary failures with backoff for up to an hour.

## Architecture

| Process | Role |
| --- | --- |
| `spacepush_poller` | Fetches both sources every minute and confirms changes (`spacepush_state:track/4`); the tracker is saved to `data/tracker.bin` |
| `spacepush_outbox` | Pending deliveries in `data/outbox.dets` |
| `spacepush_apns` | Sends due deliveries, at most 20 at a time, each with a 15 s deadline |
| `spacepush_registry` | Device registrations in `data/registry.dets`, with an ETS topic index |
| `spacepush_ratelimit` | Requests per client and minute for the HTTP API |

Pending changes and deliveries are on disk, so a restart of a process or of the whole service loses no work.

## Requirements

- Erlang/OTP 28
- rebar3

## Run locally

    rebar3 shell

Without an APNs key, notifications are only logged. Device registrations are stored in `data/registry.dets`.

## API

Register a device (replaces an earlier registration of the same token):

    PUT /v1/devices/<hex device token>
    Content-Type: application/json

    {
      "environment": "sandbox",
      "subscriptions": [
        {"endpoint": "https://status.mainframe.io/api/spaceInfo", "room": "radstelle"},
        {"endpoint": "https://api.nerd2nerd.org/status.json"}
      ]
    }

`environment` is `sandbox` for development builds and `production` for App Store and TestFlight builds. `room` defaults to `space`; only Mainframe has other rooms. Answers `204`.

Remove a device:

    DELETE /v1/devices/<hex device token>

`GET /health` answers `200` for uptime checks. Requests are limited per client (30 per minute by default). At most 10 000 devices can be registered; registrations not renewed within 60 days expire, and the apps renew on every launch.

## Notifications

The alert title is the space name (and the room for Mainframe rooms). The body is a localization key the apps resolve:

| State | Key |
| --- | --- |
| open | `PUSH_STATE_OPEN` |
| closed | `PUSH_STATE_CLOSED` |
| keyholder | `PUSH_STATE_KEYHOLDER` |
| member | `PUSH_STATE_MEMBER` |
| open+ | `PUSH_STATE_OPEN_PLUS` |
| closing | `PUSH_STATE_CLOSING` |

The payload also carries `endpoint`, `room` and `state`.

## Configuration

All settings live in the `spacepush` application environment (see `src/spacepush.app.src` for defaults and `config/sys.config` for local development):

| Key | Purpose |
| --- | --- |
| `apns_key_file` | Path to the `.p8` key; `undefined` only logs notifications |
| `apns_key_id`, `apns_team_id` | Key ID and team ID from the Apple Developer account |
| `apns_topic` | App bundle ID |
| `http_ip`, `http_port` | Listener; defaults to `127.0.0.1:8080` behind a reverse proxy |
| `trust_proxy` | Rate-limit by `X-Forwarded-For`; enable only behind a trusted proxy |
| `poll_interval_ms`, `debounce_s` | Polling interval and how long a new state must hold |
| `max_data_age_s` | Aggregator data older than this counts as unknown |
| `max_registrations`, `registration_ttl_days` | Cap and expiry for device registrations |
| `delivery_ttl_ms` | How long a delivery is retried (also sent as `apns-expiration`) |
| `apns_max_in_flight`, `apns_request_timeout_ms` | Concurrent APNs requests and their deadline |
| `registry_file`, `outbox_file`, `tracker_file` | Where state is kept on disk |

## Tests

    rebar3 eunit

## License

MIT, see [LICENSE](LICENSE).
