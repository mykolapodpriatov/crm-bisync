# crm-bisync

**Bidirectional CRM sync engine that survives the hard cases.**

Two-way syncing between CRMs is easy to demo and hard to run. The demo breaks the
first time a webhook is delivered twice, or your own write comes back as an
inbound change and starts an echo loop, or the provider returns `429` mid-batch
and half a page is already written.

`crm-bisync` is a small Go daemon built around exactly those cases: origin
tagging and echo suppression, idempotent writes, delta sync with watermarks and
an overlap window, explicit conflict policies, rate-limit-aware scheduling, and
a dead-letter queue you can inspect and replay.

Status: **early, in active development.** See [ROADMAP](#roadmap).

## Why this exists

Most integration code treats sync as "fetch changed records, write them to the
other side". That is the happy path, and it is maybe 20% of the work. The other
80% is:

| Failure | What goes wrong without handling | What `crm-bisync` does |
|---|---|---|
| Echo loop | Your write fires the peer's webhook, which writes back, forever | Origin tagging plus a short-TTL write log; self-caused events are dropped before the pipeline |
| Duplicate webhook delivery | The same change is applied twice, counters double | Delivery-ID dedupe plus deterministic idempotency keys on every write |
| Clock skew on watermarks | Records modified during a poll are silently skipped forever | Watermarks advance with a configurable overlap window and are only committed after the batch durably lands |
| Rate limits | A `429` mid-batch leaves the sync half applied | Per-connector token bucket, `Retry-After` aware backoff, resumable batches |
| Concurrent edits | Last writer silently wins, quietly losing data | Explicit per-mapping conflict policy, including field-level and manual review |
| Schema drift | A renamed field breaks writes at 3am | Mapping validation at startup and in `doctor`, with a clear failure instead of a partial write |

## Planned shape

```
crm-bisync doctor    # verify credentials, scopes, remote schema, mapping validity
crm-bisync plan      # dry run: print every write that would happen, change nothing
crm-bisync run       # the daemon: pollers, webhook receiver, workers, /metrics
crm-bisync dlq       # list, inspect and replay dead-lettered work items
crm-bisync chaos     # run the fault-injection harness against in-memory fake CRMs
```

## Connectors

v1 targets two connectors that anyone can actually run:

- **HubSpot** — free developer test accounts, and currently on date-based API
  versioning (`/crm/objects/2026-03/`), so the adapter has to deal with a real
  live migration rather than a frozen v3 snapshot.
- **Twenty** — open source and self-hostable via Docker, with a REST API and
  webhooks, which means end-to-end tests can run in CI without a paid account.

A `fake` connector with fault injection backs the test suite.

## Roadmap

v1 scope, follow-up work and known non-goals are tracked in
[GitHub issues](https://github.com/mykolapodpriatov/crm-bisync/issues).

## License

MIT
