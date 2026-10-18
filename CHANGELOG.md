# Changes

## 2.1.0

- Poll subscribed endpoints every two minutes and other directory spaces every ten minutes. Spread initial background deadlines, prioritize subscribers in bounded batches and observe fresh on-demand read generations without extra requests. Mainframe rooms follow the same subscription tier; read activity no longer enables frequent polling (`watch_window_ms` is unused). Keep shared cache admission, failure backoff and observation expiry unchanged.

- Add server-local `POST /v1/test?endpoint=...&room=space` to broadcast a fixed test alert to active subscribers. Keep tests separate from state notifications in the persistent outbox and APNs collapse identifiers, apply a per-topic cooldown and retain registration/environment checks. Older releases discard pending test records on downgrade.

## 2.0.0

- Apply `max_data_age_s` to space and room reads as well as directory states. Expired or future-dated fallback documents answer 502; current documents expose fetch, age-limit and expiry headers and require cache revalidation. Directory uses the newer current observation regardless of the polling watch window. Summary adds known/unknown coverage and `newest_as_of`; `as_of` now describes the oldest included current observation.
- Validate device tokens and rooms consistently at the API, registry and sender boundaries. Empty subscriptions unregister a device persistently.
- Persist the first admission time and APNs acceptance per registration. Confirm only matching completed responses, preserve acceptance across same-environment renewals, and expose aggregate confirmation counts in local statistics. Older records migrate to format 3 as unconfirmed; downgrades require a pre-upgrade registry backup.
- Cap unconfirmed admission at 1000 registrations and expire it after a fixed 60 days. Renewal does not extend that deadline; expired records are excluded from reads and sends before periodic cleanup. Accepted registrations retain normal renewal, while an environment change requires a provisional slot. Preserve existing unexpired records above a lowered cap and reclaim expired capacity before admitting new registrations.
- Bound statistics ingress and weekly version dimensions; recover valid stored counters and metadata safely.
- Share bounded fetch workers, deadlines, admission and failure backoff between reads and polling. Consume each successful response generation once and preserve the last usable response when a refresh fails.
- Recheck directory freshness on every read, reject unusable Mainframe responses, and revalidate directory/summary responses.
- Share APNs dispatch capacity fairly between environments and keep connection retries under sender control. HTTP/2 closing requests return to the outbox instead of being replayed by Gun.
- Give replacement deliveries a full lifetime, publish registration renewals without lookup gaps, and recover tracker/outbox hand-offs across process and VM crashes.
- Apply cooldown to new states per device and room from the last dispatch attempt; retries use their own backoff, with at least 15 minutes after APNs 5xx responses.
- Validate saved state and impose a configurable serialized snapshot/intent size limit. Oversized files and filesystem failures stop explicitly; malformed nontransactional records are normalized or removed.

The outbox commit marker requires a consistent pre-upgrade tracker/outbox backup for a downgrade. Local and CI verification do not establish deployed server behavior, host power-loss durability or end-to-end exactly-once APNs delivery.
