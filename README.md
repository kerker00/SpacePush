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
      "platform": "ios",
      "subscriptions": [
        {"endpoint": "https://status.mainframe.io/api/spaceInfo", "room": "radstelle"},
        {"endpoint": "https://api.nerd2nerd.org/status.json"}
      ]
    }

`environment` is `sandbox` for development builds and `production` for App Store and TestFlight builds. `platform` (`ios` or `macos`) is optional and only used for the statistics. `room` defaults to `space`; only Mainframe has other rooms. Every endpoint must be listed in the SpaceAPI directory. Answers `204`, or `400` for invalid input or an unknown endpoint, `408` if the body does not arrive within 10 seconds, `413` for a body over 16 KB, `429` when rate-limited and `503` when the registry is full or the directory is not loaded yet.

Remove a device:

    DELETE /v1/devices/<hex device token>

Read the current state, in the shapes the apps already decode:

    GET /v1/directory                    the listed spaces, in the aggregator's format
    GET /v1/spaces?endpoint=<url>        a space's SpaceAPI document, at most a minute old
    GET /v1/mainframe/rooms              Mainframe's openState response

Unknown endpoints answer `404`; a space that cannot be fetched and has nothing cached answers `502`, and too many spaces fetched at once `503`.

`GET /health` answers `200` for uptime checks. Requests are limited per client and minute: 30 writes and 300 reads by default. At most 10 000 devices can be registered; registrations not renewed within 60 days expire, and the apps renew on every launch.

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
| `apns_keys` | One `.p8` key per APNs environment: `#{sandbox => {File, KeyId}, production => {File, KeyId}}`. An environment without a key only logs its notifications |
| `apns_key_file`, `apns_key_id` | One key for both environments ("Sandbox & Production"), used when `apns_keys` is not set; `undefined` only logs notifications |
| `apns_team_id` | Team ID from the Apple Developer account |
| `apns_topic` | App bundle ID |
| `http_ip`, `http_port` | Listener; defaults to `127.0.0.1:8080` behind a reverse proxy |
| `trust_proxy` | Rate-limit by `X-Forwarded-For`; enable only behind a trusted proxy |
| `poll_interval_ms`, `debounce_s` | Polling interval and how long a new state must hold |
| `aggregator_url`, `directory_refresh_ms`, `directory_max_bytes` | Source of the list of spaces, how often it is reloaded and its size limit |
| `max_data_age_s` | Aggregator states older than this are not shown in the directory |
| `watch_window_ms` | How long a space stays fetched after an app asked for it |
| `fetch_concurrency`, `fetch_timeout_ms`, `max_body_bytes` | Limits for fetching spaces |
| `cache_max_age_ms`, `on_demand_fetch_limit` | Freshness of read API answers and parallel on-demand fetches |
| `notification_cooldown_ms` | Minimum time between notifications for one space or room |
| `rate_limit_per_minute`, `read_rate_limit_per_minute`, `http_max_connections` | Limits for clients |
| `max_registrations`, `registration_ttl_days` | Cap and expiry for device registrations |
| `delivery_ttl_ms` | How long a delivery is retried (also sent as `apns-expiration`) |
| `apns_max_in_flight`, `apns_request_timeout_ms` | Concurrent APNs requests and their deadline |
| `registry_file`, `outbox_file`, `tracker_file`, `directory_file`, `stats_file` | Where state is kept on disk |

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
