# SpacePush

Sends push notifications to the [SpaceState](https://github.com/kerker00/SpaceState) apps when a hackerspace opens or closes.

An Erlang/OTP application that:

- polls the [SpaceAPI](https://spaceapi.io) aggregator (`api.spaceapi.io`) for the state of every listed space,
- also polls Mainframe Oldenburg's `openState` endpoint for its rooms (Radstelle, 3D Lab, Machining) and finer states (members only, closing, …),
- waits until a new state has held for two minutes, so short flaps send nothing,
- sends the notification over APNs HTTP/2 with token-based authentication to every device that subscribed to that space or room.

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

`GET /health` answers `200` for uptime checks. Requests are limited per client (30 per minute by default).

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
| `poll_interval_ms`, `debounce_ms` | Polling interval and how long a state must hold |

## Tests

    rebar3 eunit

## License

MIT, see [LICENSE](LICENSE).
