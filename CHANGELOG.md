# Changes

## Unreleased

- Validate device tokens and rooms consistently at the API, registry and sender boundaries. Empty subscriptions unregister a device persistently.
- Persist the first admission time and APNs acceptance per registration. Confirm only matching completed responses, preserve acceptance across same-environment renewals, and expose aggregate confirmation counts in local statistics. Older records migrate to format 3 as unconfirmed; downgrades require a pre-upgrade registry backup.
- Bound statistics ingress and weekly version dimensions; recover valid stored counters and metadata safely.
- Share bounded fetch workers, deadlines, admission and failure backoff between reads and polling. Consume each successful response generation once and preserve the last usable response when a refresh fails.
- Recheck directory freshness on every read, reject unusable Mainframe responses, and revalidate directory/summary responses.
- Share APNs dispatch capacity fairly between environments and keep connection retries under sender control. HTTP/2 closing requests return to the outbox instead of being replayed by Gun.
- Give replacement deliveries a full lifetime, publish registration renewals without lookup gaps, and recover tracker/outbox hand-offs across process and VM crashes.
- Apply cooldown to new states per device and room from the last dispatch attempt; retries use their own backoff, with at least 15 minutes after APNs 5xx responses.
- Validate saved state and impose a configurable serialized snapshot/intent size limit. Oversized files and filesystem failures stop explicitly; malformed nontransactional records are normalized or removed.

The outbox commit marker requires a consistent pre-upgrade tracker/outbox backup for a downgrade. Local and CI verification do not establish deployed server behavior, host power-loss durability or end-to-end exactly-once APNs delivery.
