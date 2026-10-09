# SpacePush

Sends push notifications to the [SpaceState](https://github.com/kerker00/SpaceState) apps when a hackerspace opens or closes, and serves the apps the spaces' current state, so each space is asked once a minute however many people use the apps.

An Erlang/OTP application that:

- loads the list of spaces from the [SpaceAPI](https://spaceapi.io) aggregator (`api.spaceapi.io`) at start and hourly; the list is also the allowlist of endpoints SpacePush talks to,
- fetches directly, every minute, only the spaces that are needed: subscribed by a device or asked for by an app within the last ten minutes,
- also fetches Mainframe Oldenburg's `openState` endpoint for its rooms (Radstelle, 3D Lab, Machining) and finer states (members only, closing, …),
- confirms a new state only when fresh data still shows it two minutes later, so short flaps send nothing,
- queues one delivery per device and topic in a persistent outbox, where a newer state replaces an unsent older one,
- sends them over APNs HTTP/2 with token-based authentication, retrying temporary failures with backoff for up to an hour.

## Architecture

| Process | Role |
| --- | --- |
| `spacepush_directory` | The list of spaces and allowlist, refreshed hourly and saved to `data/directory.bin` |
| `spacepush_cache` | Latest response per space; fetches on demand for the read API, once per space however many requests wait |
| `spacepush_poller` | Fetches the needed spaces every minute, at most 10 at a time, skipping failing ones with backoff, and confirms changes (`spacepush_state:track/4`); the tracker is saved to `data/tracker.bin` |
| `spacepush_outbox` | Pending deliveries in `data/outbox.dets` |
| `spacepush_apns` | Sends due deliveries, at most 20 at a time, each with a 15 s deadline |
| `spacepush_registry` | Device registrations in `data/registry.dets`, with an ETS topic index |
| `spacepush_ratelimit` | Requests per client and minute for the HTTP API |

Pending changes and deliveries are synced to disk before they count as accepted, so a restart of a process or of the whole service, even an abrupt one, loses no work. Persistent formats carry a version; older records are migrated on start.

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

`environment` is `sandbox` for development builds and `production` for App Store and TestFlight builds. `room` defaults to `space`; only Mainframe has other rooms. Every endpoint must be listed in the SpaceAPI directory. Answers `204`, or `400` for invalid input or an unknown endpoint, `408` if the body does not arrive within 10 seconds, `413` for a body over 16 KB, `429` when rate-limited and `503` when the registry is full or the directory is not loaded yet.

Remove a device:

    DELETE /v1/devices/<hex device token>

Read the current state, in the shapes the apps already decode:

    GET /v1/directory                    the listed spaces, in the aggregator's format
    GET /v1/spaces?endpoint=<url>        a space's SpaceAPI document, at most a minute old
    GET /v1/mainframe/rooms              Mainframe's openState response

Unknown endpoints answer `404`; a space that cannot be fetched and has nothing cached answers `502`, and too many spaces fetched at once `503`.

`GET /health` answers `200` for uptime checks. Requests are limited per client and minute: 30 writes and 300 reads by default. At most 10 000 devices can be registered; registrations not renewed within 60 days expire, and the apps renew on every launch.

## Security

Spaces run their own endpoints, and clients are anonymous, so SpacePush treats both as untrusted:

- Only endpoints listed in the SpaceAPI directory are fetched or accepted in subscriptions.
- Each request resolves the host itself and connects only to a public address, pinned for the request; loopback, private, link-local and similar addresses are refused, also when DNS points there. TLS is verified against the host name. Redirects are not followed.
- Responses are read in chunks and aborted beyond 256 KB or after 10 seconds, and are cached only if they parse as a space.
- Each space is fetched at most once a minute; at most 20 on-demand fetches run at a time.
- Notification titles are stripped of control and direction characters and shortened to 64 characters. Notifications for one space or room are at least five minutes apart; changes in between are merged into the latest.
- Passed-through documents are served with `X-Content-Type-Options: nosniff`.
- Behind the proxy, the rate limit uses the last `X-Forwarded-For` entry, the one the proxy added. The listener accepts at most 1024 connections.

Not covered: a botnet registering many devices can fill the registry (Apple's App Attest would tie registrations to genuine app installs), and spaces that only offer plain HTTP can be altered in transit.

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
| `aggregator_url`, `directory_refresh_ms` | Source of the list of spaces and how often it is reloaded |
| `max_data_age_s` | Aggregator states older than this are not shown in the directory |
| `watch_window_ms` | How long a space stays fetched after an app asked for it |
| `fetch_concurrency`, `fetch_timeout_ms`, `max_body_bytes` | Limits for fetching spaces |
| `cache_max_age_ms`, `on_demand_fetch_limit` | Freshness of read API answers and parallel on-demand fetches |
| `notification_cooldown_ms` | Minimum time between notifications for one space or room |
| `rate_limit_per_minute`, `read_rate_limit_per_minute`, `http_max_connections` | Limits for clients |
| `max_registrations`, `registration_ttl_days` | Cap and expiry for device registrations |
| `delivery_ttl_ms` | How long a delivery is retried (also sent as `apns-expiration`) |
| `apns_max_in_flight`, `apns_request_timeout_ms` | Concurrent APNs requests and their deadline |
| `registry_file`, `outbox_file`, `tracker_file`, `directory_file` | Where state is kept on disk |

## Deploy on Uberspace 7

Uberspace 7 runs CentOS 7, whose Erlang and OpenSSL are too old, so both are built once in the home directory. Templates for the configuration and the service are in `deploy/uberspace/`.

1. **OpenSSL 3.5** (static, ~10 min) into `~/opt/openssl-3.5.9`:

        ./Configure linux-x86_64 --prefix=$HOME/opt/openssl-3.5.9 --libdir=lib no-shared no-tests -fPIC
        make -j2 && make install_sw

2. **Erlang/OTP 28** (~30 min) into `~/opt/otp-28.5.0.7`, linked against it:

        ./configure --prefix=$HOME/opt/otp-28.5.0.7 --with-ssl=$HOME/opt/openssl-3.5.9 --disable-dynamic-ssl-lib \
          --without-javac --without-wx --without-odbc --without-observer --without-debugger --without-et
        make -j2 && make install

3. **rebar3** in `~/bin`, with the new Erlang first in `PATH`.
4. **Release**: `rebar3 as prod release` in a checkout, then copy `_build/prod/rel/spacepush` to `~/spacepush/release`. It contains the Erlang runtime.
5. **Configuration**: `~/spacepush/sys.config` and `~/spacepush/vm.args` from the templates, the APNs key in `~/spacepush/secrets/` (mode 600), data in `~/spacepush/data/`.
6. **Service**: `deploy/uberspace/spacepush.ini` to `~/etc/services.d/`, then `supervisorctl reread && supervisorctl update`.
7. **Web backend**: `uberspace web backend set push.grafixmafia.net --http --port 52184`; the port stays closed to the outside, Uberspace's proxy forwards HTTPS to it.

Check with `curl https://push.grafixmafia.net/health`. Logs: `supervisorctl tail -f spacepush`.

To update: build a new release, copy it to `~/spacepush/release.new`, then `supervisorctl stop spacepush`, swap the directories and `supervisorctl start spacepush`. Data, configuration and key live outside the release.

## Tests

    rebar3 eunit

## License

MIT, see [LICENSE](LICENSE).
