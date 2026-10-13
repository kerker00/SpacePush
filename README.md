# SpacePush

Sends push notifications to the [SpaceState](https://github.com/kerker00/SpaceState) apps when a hackerspace opens or closes, and serves the apps the spaces' current state, so by default each space is asked at most once a minute however many people use the apps.

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
| `spacepush_poller` | Requests needed spaces through the shared cache workers every minute, at most 10 at a time, and confirms changes (`spacepush_state:track/4`); the tracker is saved to `data/tracker.bin` |
| `spacepush_outbox` | Pending deliveries in `data/outbox.dets`; replacement states receive a fresh lifetime while keeping a live delivery's scheduled slot |
| `spacepush_apns` | Sends due deliveries, at most 20 at a time, each with a 15 s deadline; selects ready environments before limiting and shares capacity fairly when both have work |
| `spacepush_registry` | Device registrations in `data/registry.dets`, with an ETS topic index; renewals atomically replace the visible registration without a lookup gap |
| `spacepush_ratelimit` | Requests per client and minute for the HTTP API |

Confirmed changes first write a versioned intent beside the tracker (`tracker.bin.intent`). The outbox syncs all delivery rows before writing and syncing its commit marker; the tracker snapshot is then saved before the intent is removed. Startup completes pending outbox work before starting APNs and restores the intended tracker before polling. Replaying an applied commit does not requeue deliveries already completed by the sender. Backups must keep the tracker, any intent and the outbox together. Persistent formats carry a version; older registration records are migrated on start.

The local regression suite covers process crashes and abrupt VM termination at the hand-off boundaries. It does not establish durability under host power loss: parent directories are not synced. An APNs request accepted remotely before its local result is recorded can still be retried after a crash; end-to-end exactly-once delivery is not promised.

Older releases do not understand the new outbox commit marker. Downgrading requires restoring a consistent pre-upgrade backup of the tracker and outbox.

### Reading saved state

Snapshots and recovery intents use safe ETF decoding and must contain exactly one term. `state_max_bytes` defaults to 64 MiB and limits both serialized input and output; compressed input also checks its declared expanded size before decoding. This is a serialized-data limit, not a bound on total process memory. Oversized files and filesystem read/write errors stop the operation with a named error and file path. A failed size check does not overwrite the previous snapshot.

Missing snapshots start with empty state. Invalid ETF and unsupported directory, tracker or statistics snapshot versions fall back to empty state; unreadable or invalid recovery intents and unsupported outbox commit markers stop startup. Fix the file access or restore a consistent backup before restarting; do not discard a pending intent independently of its tracker and outbox.

Directory recovery keeps at most 100,000 entries with nonempty UTF-8 endpoints of at most 4096 bytes. Missing or invalid optional metadata becomes `null`; geographic coordinates and observation timestamps are checked. Tracker recovery keeps valid topics, known states and bounded nonnegative candidate timestamps. Unexpected fields are removed.

Statistics recovery keeps valid recent calendar dates and nonnegative integer counters, at most 256 counters per day with keys of at most 128 bytes. Weekly dimensions retain the configured limits plus `other`; malformed groups become empty maps. At most 32 last-success sources are kept, with bounded names and valid nonfuture timestamps. Format 1 counter migration remains supported, and valid totals survive within these limits.

Registry recovery validates tokens, environments, topics, versions and platforms; supported older formats are migrated, while malformed or empty registrations are removed. Outbox recovery removes malformed delivery records, checks their identity, payload and collapse id, and keeps valid work across restarts. Delivery payloads are bounded to 4096 bytes. Diagnostics report error categories or dropped counts, without complete registration/delivery records or device tokens.

Registration format 3 also retains the provisional admission time and APNs acceptance.
Only a complete HTTP 200 response for the current token, environment and
registration version marks it accepted. Failures, dry runs and late responses
for replaced registrations do not. Acceptance survives renewal in the same
environment and restart; switching environment resets it. The local statistics
expose `devices.by_confirmation` as `accepted_by_apns` or `unconfirmed`.
They also report `devices.unconfirmed_cap` (at most the total registration cap).
APNs acceptance does not prove receipt by the device or a genuine app installation.
Older records migrate as unconfirmed with their saved registration time. Back up
the registry before upgrading; older releases do not understand format 3 and
require its pre-upgrade backup when downgrading.

## Continuous integration

GitHub CI compiles production code and the test profile and runs the full EUnit suite on Ubuntu 24.04 with patched OTP 28.5.0.7 and 29.1.1 and rebar3 3.27.3. The checkout/setup actions are pinned to verified release commits, use read-only repository permissions and do not persist checkout credentials.

Normal PR updates and pushes to `dev`/`master` run the matrix. Draft PRs skip jobs; marking a draft ready triggers them. The tests use mocked responses, temporary files, local loopback HTTP/2 peers and disposable crash-test VMs, without production configuration or genuine pushes. CI validates its runner's source and runtime, not the deployed host's version, proxy settings or release.

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
      "platform": "ios",
      "subscriptions": [
        {"endpoint": "https://status.mainframe.io/api/spaceInfo", "room": "radstelle"},
        {"endpoint": "https://api.nerd2nerd.org/status.json"}
      ]
    }

`environment` is `sandbox` for development builds and `production` for App Store and TestFlight builds. `platform` (`ios` or `macos`) is optional and only used for the statistics. `room` defaults to `space`; only Mainframe has other rooms. Every endpoint must be listed in the SpaceAPI directory. Answers `204`, or `400` for invalid input or an unknown endpoint, `408` if the body does not arrive within 10 seconds, `413` for a body over 16 KB, `429` when rate-limited and `503` when the registry is full or the directory is not loaded yet.

Remove a device:

    DELETE /v1/devices/<hex device token>

A valid `PUT` with `"subscriptions": []` also removes the device and answers
`204`, including when the directory is unavailable or registration capacity is
full. It does not create a registration. Older persisted registrations without
subscriptions are removed on startup.

Read the current state, in the shapes the apps already decode:

    GET /v1/directory                    the listed spaces, in the aggregator's format
    GET /v1/spaces?endpoint=<url>        a space's last successful SpaceAPI document
    GET /v1/mainframe/rooms              Mainframe's openState response
    GET /v1/summary                      open-space count, coverage and observation times

`/v1/directory` uses the newer current observation from a direct fetch or the hourly aggregator data. Every response checks the original observation time against `max_data_age_s` (900 seconds by default); expired states omit the open flag while the space's name, location and allowlisted endpoint remain available. Future timestamps do not provide a current state. The watch window controls polling activity, not observation freshness. All successful read responses use `Cache-Control: no-cache` so clients revalidate them, and expose the configured limit in `X-SpacePush-Max-Data-Age` (seconds). Summary also exposes this header to cross-origin web clients.

`/v1/summary` returns `open`, `total`, `known`, `unknown`, `as_of` and `newest_as_of`. It counts the same current opening flags as `/v1/directory`; `known` includes both current open and closed states, and `unknown = total - known`. Unknown spaces are not closed spaces. **`as_of` now describes the oldest included observation**, so one newly fetched space cannot make the whole count appear newly observed; `newest_as_of` describes the newest. Both are RFC 3339 UTC timestamps, or `null` when no current states are known. These times describe the known subset, not unknown spaces. It carries `Access-Control-Allow-Origin: *`, so web pages such as the project site can show the number live.

Space and Mainframe document responses carry `X-SpacePush-Fetched-At` (last successful fetch, Unix seconds), `X-SpacePush-Max-Data-Age` (configured freshness limit in seconds) and `X-SpacePush-Expires-At` (fetch time plus that limit, Unix seconds). A failed or deferred refresh can serve the last good document only while its original fetch is at most `max_data_age_s` old; an expired or future-dated fallback answers `502` with `error: "stale_data"`. This applies to both spaces and rooms. Timestamp headers round down to whole seconds; clients should expire a retained answer conservatively at the advertised expiry. Mainframe responses without any usable room are treated as failed refreshes.

A successful fetch records when SpacePush received usable data, not when a physical door was measured. SpaceAPI `state.lastchange` and Mainframe room timestamps describe state changes and do not reset this observation time. A client's successful read and most recent attempt are separate times. These headers provide the server policy for clients to adopt; they do not automatically expire data already retained in an app, widget, CLI cache or website. On a read error, a client may retain a labelled last known value, but must not present it as a newly observed current state or silently classify unknown as closed.

Unknown endpoints answer `404`; a space that cannot be fetched and has nothing cached answers `502`, and too many spaces fetched at once `503`.

`GET /health` answers `200` for uptime checks. Requests are limited per client and minute: 30 writes and 300 reads by default. At most 10 000 devices can be registered; registrations not renewed within 60 days expire, and the apps renew on every launch.

At most 1 000 registrations may be admitted without APNs acceptance. Unconfirmed
registrations additionally expire exactly 60 days after their admission, even
when renewed or switched between environments. An accepted registration renews
under the normal inactivity rule. Switching an accepted registration to another
APNs environment needs an available unconfirmed slot and starts a fresh 60-day
provisional window; refusal leaves its current registration intact. Exhausting
either admission cap answers 503. Empty subscriptions and deletion release a slot.

Expired records are hidden from lookups, statistics, polling subscriptions and
delivery selection immediately. Disk cleanup runs at startup, hourly and before
new admission. Existing unexpired records above a lowered cap are retained and
can renew without extending their provisional deadline; further admission waits
for capacity. Legacy records use their saved registration time as admission time,
and restarting does not reset the deadline. A legitimate subscription with no
accepted notification also expires; a later app launch may register it again
subject to capacity. APNs acceptance frees its provisional slot.

## Statistics

For monitoring, SpacePush keeps daily usage statistics and serves them as JSON on the server itself:

    curl -s http://127.0.0.1:<port>/v1/stats?days=14

The endpoint answers only requests from a loopback address that did not pass a proxy (no `X-Forwarded-For`); through the public domain it answers `404`. It reports:

- **Devices:** registered devices in total, per APNs environment, per platform and per subscribed space or room.
- **Installs:** active app installs today, this week and this month, the last 10 days, 12 weeks and 13 months, and the app versions, platforms and OS versions of this week.
- **Per day** (`days`): requests per endpoint, widget and other requests, rate-limited requests, new, renewed, removed, expired and invalid registrations, deliveries by APNs result, fetches of the spaces and of Mainframe, directory refreshes, poll rounds with their duration, and confirmed state changes.
- **Current state:** pending deliveries, last successful fetch per source, version, uptime, memory and process count.

Installs are counted without an identifier. With its first request of a day, ISO week and month, the app sends `X-SpaceState-First` naming the periods it opens, such as `day, week, month`, together with a user agent such as `SpaceState/2.0.0 (iOS 26.0)`; SpacePush adds one per named period. Widgets send no such header. Client addresses are not recorded. Daily counters are kept for 430 days, weekly version tallies for 13 weeks. Days follow the server's local time.

## Security

Spaces run their own endpoints, and clients are anonymous, so SpacePush treats both as untrusted:

- Only endpoints listed in the SpaceAPI directory are fetched or accepted in subscriptions.
- Each request resolves the host itself and connects only to a public address, pinned for the request; loopback, private, link-local and similar addresses are refused, also when DNS points there. TLS is verified against the host name. Redirects are not followed.
- Responses are read in chunks and aborted beyond 256 KB or after 10 seconds, and are cached only if they parse as a space.
- Polling and read requests share each space's fetch: attempts start at least a minute apart by default, and failures back off up to 30 minutes. At most 20 combined fetches run at a time. Reads can keep serving the last good response with its original fetch timestamp while refresh is deferred. The poller observes each successful response once, including results obtained by readers; repeated cached responses, responses beyond `max_data_age_s` and failed refreshes do not confirm state changes.
- Notification titles are stripped of control and direction characters and shortened to 64 characters. Notifications for one space or room are at least five minutes apart; changes in between are merged into the latest.
- Passed-through documents are served with `X-Content-Type-Options: nosniff`.
- Behind the proxy, the rate limit uses the last `X-Forwarded-For` entry, the one the proxy added. The listener accepts at most 1024 connections.

Clients remain anonymous; APNs acceptance does not establish installation
authenticity. Space endpoints that offer plain HTTP lack authenticated transport.

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
| `apns_keys` | One `.p8` key per APNs environment: `#{sandbox => {File, KeyId}, production => {File, KeyId}}`. An environment without a key only logs its notifications |
| `apns_key_file`, `apns_key_id` | One key for both environments ("Sandbox & Production"), used when `apns_keys` is not set; `undefined` only logs notifications |
| `apns_team_id` | Team ID from the Apple Developer account |
| `apns_topic` | App bundle ID |
| `http_ip`, `http_port` | Listener; defaults to `127.0.0.1:8080` behind a reverse proxy |
| `trust_proxy` | Rate-limit by `X-Forwarded-For`; enable only behind a trusted proxy |
| `poll_interval_ms`, `debounce_s` | Polling interval and how long a new state must hold |
| `aggregator_url`, `directory_refresh_ms`, `directory_max_bytes` | Source of the list of spaces, how often it is reloaded and its size limit |
| `max_data_age_s` | Maximum observation age for directory/summary states and space/room document reads |
| `watch_window_ms` | How long a space stays fetched after an app asked for it |
| `fetch_concurrency`, `fetch_timeout_ms`, `max_body_bytes` | Limits for fetching spaces |
| `fetch_min_interval_ms`, `fetch_backoff_max_ms` | Shared minimum time between attempts per endpoint, and maximum failure backoff |
| `cache_max_age_ms`, `on_demand_fetch_limit`, `on_demand_waiter_limit` | Time before a read attempts refresh, combined parallel refreshes and total waiting callers |
| `notification_cooldown_ms` | Minimum time from the last dispatch attempt to a new state for the same device and room; retries of the same delivery use their own backoff. Kept in memory, reset on restart |
| `rate_limit_per_minute`, `read_rate_limit_per_minute`, `http_max_connections` | Limits for clients |
| `max_registrations`, `registration_ttl_days` | Cap and expiry for device registrations |
| `max_unconfirmed_registrations`, `unconfirmed_registration_ttl_days` | Additional admission cap (1000) and fixed lifetime from provisional admission (60 days); renewals do not extend it |
| `delivery_ttl_ms` | How long a delivery is retried (also sent as `apns-expiration`) |
| `apns_max_in_flight`, `apns_request_timeout_ms` | Concurrent APNs requests and their deadline |
| `registry_file`, `outbox_file`, `tracker_file`, `directory_file`, `stats_file` | Where state is kept on disk |
| `state_max_bytes` | Maximum serialized snapshot or recovery intent size; default 67108864 bytes, also checked for expanded compressed ETF |

## Build and regression checks

For local verification, compile production code and the test profile, then run the EUnit regressions:

```sh
env -u DEBUG rebar3 compile
env -u DEBUG rebar3 as test compile
env -u DEBUG rebar3 eunit
```

The suite uses temporary data under `_build/test/tmp/`, mocked upstream/APNs responses, local loopback HTTP/2 peers and disposable Erlang VMs that are halted for crash tests. It needs permission to open local sockets and start those VMs, but no production configuration, APNs key or real push recipients. These checks validate the source on the test machine; they do not verify the runtime, proxy settings or release running on a different host. [CHANGELOG.md](CHANGELOG.md) describes the pending release changes.

## Runtime boundaries

`trust_proxy` is false by default. Enable it only for a listener reachable through a trusted proxy that appends the actual client address to `X-Forwarded-For`; earlier entries are supplied by the client. Verify the deployed routing and listener exposure separately. The provided Uberspace template is a configuration example, not evidence of the live host's settings.

`GET /v1/stats` accepts only a loopback peer without `X-Forwarded-For`, `Forwarded` or `X-Real-IP`; other requests receive 404. Keep the proxy's forwarding headers intact. `GET /v1/summary` is the public aggregate endpoint. The device token in `/v1/devices/:token` authorizes registration, renewal and removal for that device; protect tokens in clients, URLs and proxy access logs. Filled-in runtime configuration and APNs keys stay outside the repository.

Outbound space fetches use the directory allowlist, accept HTTP/HTTPS without redirects, check all resolved addresses and pin a permitted address for the connection. TLS verifies the hostname and trust chain. These application checks complement host-level network restrictions. Validate the actual deployed configuration and runtime separately; a code path alone is not an operational acceptance result.

Budget persistent storage for registrations, up to 50 topics per device, pending deliveries and retained statistics, plus snapshot temporary files and recovery intents. Admission caps do not imply a fixed disk or process-memory ceiling. Monitor space and permissions for the configured data directory. Keep tracker, pending intent and outbox in one consistent backup set after a graceful stop. A live backup strategy needs its own application-consistency and recovery validation. Restoration and host power-loss testing are operator tasks.

`GET /health` is liveness only: 200 shows that the HTTP service answered. Polling freshness, current last-success timestamps, queue progress and APNs acceptance require their own checks. Production acceptance must identify the deployed commit and exact OTP/SSL versions and exercise a controlled state change on actual clients.

## Deploy on Uberspace 7

Production runs on an [Uberspace 7](https://manual.uberspace.de/) account and answers at `https://push.grafixmafia.net`. Uberspace 7 is CentOS 7: its Erlang (OTP 21) and OpenSSL (1.0.2) are far too old, so a current OpenSSL and Erlang/OTP 28 are built once in the home directory. Nothing outside the home directory changes. Templates for the configuration and the service are in `deploy/uberspace/`.

All commands run on the Uberspace host unless they say otherwise. `<user>` and `<host>` stand for the account name and host (`<host>.uberspace.de`). Long builds are best run inside `tmux`, which survives a dropped SSH connection (`tmux attach` to return).

### Layout on the host

| Path | Contents |
| --- | --- |
| `~/opt/openssl-3.5.9` | OpenSSL, static, only used to build Erlang |
| `~/opt/otp-28.5.0.7` | Erlang/OTP 28 |
| `~/bin/rebar3` | Build tool |
| `~/spacepush/src` | Checkout of this repository |
| `~/spacepush/release` | The running release, including the Erlang runtime |
| `~/spacepush/sys.config`, `~/spacepush/vm.args` | Production configuration (mode 600) |
| `~/spacepush/secrets/` | APNs key (directory 700, key 600) |
| `~/spacepush/data/` | Directory list, registrations, outbox, tracker |
| `~/etc/services.d/spacepush.ini` | The supervisord service |

Release, configuration, key and data are separate, so a new release replaces only `~/spacepush/release`.

### 1. OpenSSL 3.5 (once, ~10 min)

Built as a static library, so Erlang does not depend on the system's OpenSSL.

    mkdir -p ~/build ~/opt && cd ~/build
    curl -LO https://github.com/openssl/openssl/releases/download/openssl-3.5.9/openssl-3.5.9.tar.gz
    curl -LO https://github.com/openssl/openssl/releases/download/openssl-3.5.9/openssl-3.5.9.tar.gz.sha256
    echo "$(grep -oE '[0-9a-f]{64}' openssl-3.5.9.tar.gz.sha256)  openssl-3.5.9.tar.gz" | sha256sum -c

Continue only if it prints `OK`:

    tar xzf openssl-3.5.9.tar.gz && cd openssl-3.5.9
    ./Configure linux-x86_64 --prefix=$HOME/opt/openssl-3.5.9 --libdir=lib no-shared no-tests -fPIC
    make -j2 && make install_sw

### 2. Erlang/OTP 28 (once, ~30 min)

    cd ~/build
    curl -LO https://github.com/erlang/otp/releases/download/OTP-28.5.0.7/otp_src_28.5.0.7.tar.gz
    curl -LO https://github.com/erlang/otp/releases/download/OTP-28.5.0.7/SHA256.txt
    grep ' otp_src_28.5.0.7.tar.gz$' SHA256.txt | sha256sum -c

Continue only if it prints `OK`:

    tar xzf otp_src_28.5.0.7.tar.gz && cd otp_src_28.5.0.7
    ./configure --prefix=$HOME/opt/otp-28.5.0.7 \
      --with-ssl=$HOME/opt/openssl-3.5.9 --disable-dynamic-ssl-lib \
      --without-javac --without-wx --without-odbc --without-observer --without-debugger --without-et
    make -j2 && make install
    ~/opt/otp-28.5.0.7/bin/erl -noshell -eval 'io:format("~s~n~p~n", [erlang:system_info(otp_release), crypto:info_lib()]), halt().'

Expect `28` and `OpenSSL 3.5.9`.

### 3. PATH and rebar3 (once)

    echo 'export PATH=$HOME/opt/otp-28.5.0.7/bin:$HOME/bin:$PATH' >> ~/.bash_profile
    source ~/.bash_profile
    mkdir -p ~/bin && cd ~/bin
    curl -LO https://github.com/erlang/rebar3/releases/download/3.27.1/rebar3
    echo "708407032479514dd68b581a0b09a68b5a781fb6f53dcf4ad81ce4ef6b92940f  rebar3" | sha256sum -c
    chmod +x rebar3 && rebar3 version

### 4. First release (once)

    mkdir -p ~/spacepush && cd ~/spacepush
    git clone --branch dev https://github.com/kerker00/SpacePush.git src
    cd src && rebar3 as prod release
    cp -a _build/prod/rel/spacepush ~/spacepush/release

The release contains the Erlang runtime (`erts-…`), so it runs on its own.

### 5. Configuration and APNs key (once)

    mkdir -p ~/spacepush/data ~/spacepush/secrets && chmod 700 ~/spacepush/secrets
    cp ~/spacepush/src/deploy/uberspace/sys.config.example ~/spacepush/sys.config
    cp ~/spacepush/src/deploy/uberspace/vm.args.example ~/spacepush/vm.args
    sed -i "s#/home/USER/#$HOME/#g; s#SANDBOXKEYID#<sandbox key id>#g; s#PRODKEYID#<production key id>#g" ~/spacepush/sys.config
    sed -i "s#^-setcookie COOKIE#-setcookie $(openssl rand -hex 32)#" ~/spacepush/vm.args
    chmod 600 ~/spacepush/sys.config ~/spacepush/vm.args

APNs keys are created per environment (Sandbox, Production) or for both. The template expects one per environment; with a single key for both, replace `apns_keys` by `apns_key_file` and `apns_key_id`. Upload each key **from the Mac**, naming the target file in full (newer `scp` fails with `dest open …: Failure` on a directory target):

    scp AuthKey_<key id>.p8 <user>@<host>.uberspace.de:spacepush/secrets/AuthKey_<key id>.p8

Then on the host:

    chmod 600 ~/spacepush/secrets/AuthKey_*.p8
    erl -noshell -pa ~/spacepush/release/lib/*/ebin -eval '
      {ok, [Config]} = file:consult(os:getenv("HOME") ++ "/spacepush/sys.config"),
      Env = proplists:get_value(spacepush, Config),
      Get = fun(Key) -> proplists:get_value(Key, Env) end,
      Keys = spacepush_apns:key_config(Get(apns_keys), Get(apns_key_file), Get(apns_key_id)),
      [spacepush_jwt:read_key(File) || {File, _KeyId} <- maps:values(Keys)],
      io:format("config ok, keys ok for ~p~n", [maps:keys(Keys)]), halt().'

Never commit the filled-in `sys.config`, `vm.args` or the key. The cookie in `vm.args` is a secret too.

### 6. Service (once)

    cp ~/spacepush/src/deploy/uberspace/spacepush.ini ~/etc/services.d/
    supervisorctl reread && supervisorctl update
    supervisorctl status spacepush
    curl -s http://127.0.0.1:52184/health; echo

supervisord restarts SpacePush after crashes and reboots.

### 7. Domain and web backend (once)

    uberspace web domain add push.grafixmafia.net

It prints the IPv4 and IPv6 address. At the DNS provider (Namecheap: Domain List → Manage → Advanced DNS) add an `A` and an `AAAA` record for host `push` with these addresses. Once `dig +short push.grafixmafia.net` shows them:

    uberspace web backend set push.grafixmafia.net --http --port 52184
    uberspace web backend list
    curl -s https://push.grafixmafia.net/health; echo

Port 52184 stays closed to the outside; Uberspace's proxy forwards HTTPS on 443 to it. Do not open it with `uberspace port add`. The certificate is issued automatically.

### Updating to a new release

Build next to the running release, then swap; the interruption is a few seconds:

    cd ~/spacepush/src && git pull
    rebar3 as prod release
    rm -rf ~/spacepush/release.new && cp -a _build/prod/rel/spacepush ~/spacepush/release.new
    supervisorctl stop spacepush
    rm -rf ~/spacepush/release.old
    mv ~/spacepush/release ~/spacepush/release.old && mv ~/spacepush/release.new ~/spacepush/release
    supervisorctl start spacepush
    sleep 20 && supervisorctl status spacepush
    curl -s https://push.grafixmafia.net/health; echo
    curl -s https://push.grafixmafia.net/v1/directory | head -c 200; echo

Data, configuration and key are untouched. If the new configuration template gained keys you need (see `src/spacepush.app.src` and the release notes), add them to `~/spacepush/sys.config` before starting; keys missing there fall back to the defaults.

To roll back, swap back to the previous release:

    supervisorctl stop spacepush
    mv ~/spacepush/release ~/spacepush/release.broken && mv ~/spacepush/release.old ~/spacepush/release
    supervisorctl start spacepush

### Updating Erlang or OpenSSL

Build the new version into its own directory under `~/opt` as in steps 1 and 2, point `PATH` in `~/.bash_profile` at it, and build and swap a release as above. The release brings its own runtime, so the old one in `~/opt` can be deleted once the new release runs.

### Operating

    supervisorctl status spacepush        # running?
    supervisorctl tail -f spacepush       # log; the level is notice, so only noteworthy events appear
    supervisorctl restart spacepush
    curl -s http://127.0.0.1:52184/v1/stats | python3 -m json.tool    # usage statistics, see Statistics

Remote shell into the running node, for inspection (`Ctrl-G q` leaves without stopping it):

    export VMARGS_PATH=~/spacepush/vm.args RELX_CONFIG_PATH=~/spacepush/sys.config
    ~/spacepush/release/bin/spacepush remote_console

The release script reads `vm.args` and `sys.config` from these two variables; the service sets them itself. The node must be named `spacepush@localhost` in `vm.args`: with a plain `-sname spacepush` it is `spacepush@<host>`, the host name resolves to the public address, where the distribution does not listen, and `remote_console` reports "Node is not running!".

### Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `remote_console` says "Node is not running!" although the service runs | `vm.args` still has `-sname spacepush`. Change it to `-sname spacepush@localhost` and restart the service. |
| `/v1/directory` answers `directory_unavailable` | The list could not be loaded. Check the log for `directory_fetch_failed`; `too_large` means `directory_max_bytes` is too small. |
| `config ok` check fails with `enoent` | A key file is not where `apns_keys` (or `apns_key_file`) says; compare `ls ~/spacepush/secrets` with the paths in `sys.config`. |
| `uberspace web backend list` says the backend is not OK | SpacePush is not running or listens on the wrong interface or port; it must listen on `0.0.0.0:52184`. |
| Warning about a post-quantum key exchange when connecting | Informational, from a newer SSH client; Uberspace 7's server does not offer it. |
| Notifications never arrive | `apns_key_rejected` names the environment whose key APNs refused: the key is not valid for that environment (Sandbox or Production) or belongs to another team. Sending for it pauses 20 minutes. Other `apns_rejected_*` entries point to the topic (`apns_topic` must be the apps' bundle ID). |

## Tests

    rebar3 eunit

## License

MIT, see [LICENSE](LICENSE).
