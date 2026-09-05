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

Status: **in active development.** The engine works and is tested against
two in-memory CRMs under injected faults; the HubSpot and Twenty adapters are
not written yet. See [ROADMAP](#roadmap).

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

## See it decide something

Nothing here needs an account anywhere. The `fake` driver is an in-memory CRM
loaded from a fixture, and it is the same connector the whole test suite runs
against, so what you see below is the engine's real reasoning and not a mock-up
of it.

```
$ crm-bisync doctor -config examples/config.json
ok    store ./state                   readable and writable
ok    connector hubspot/contact       4 fields
ok    connector twenty/contact        4 fields
ok    mapping contact:hubspot-twenty  4 fields, bidirectional
ok    watermark hubspot/contact       not set yet; the next run backfills
ok    watermark twenty/contact        not set yet; the next run backfills
ok    queue dead-letter queue         empty
ok    queue review queue              empty

All 8 checks passed.
```

`doctor` writes nothing and is meant to be run against production. It is where
a renamed property, an expired token or an unwritable state directory turns up
at a moment when somebody is looking, rather than at three in the morning as a
write that half-lands.

```
$ crm-bisync plan -config examples/config.json
contact:hubspot-twenty
  create on twenty
    from hubspot:contact/hs-2
    created_at = "2026-02-11"
    email = "bo@northwind.test"
    first_name = "Bo"
    last_name = "Vance"
  create on twenty
    from hubspot:contact/hs-3
    created_at = "2026-03-02"
    email = "cass@fabrikam.test"
    first_name = "Cass"
    last_name = "Ibarra"
  create on hubspot
    from twenty:contact/tw-9
    email = "dee@contoso.test"
    first_name = "Dee"
    last_name = "Okafor"

Waiting for a decision (1)
  hubspot:contact/hs-1
    both sides changed 0s apart, inside the 2s clock tolerance, so which is newer is not knowable

3 to create, 1 waiting for a decision.
```

Four things in that output are the whole design:

- **`created_at` appears on the writes going to Twenty and not on the one going
  to HubSpot.** It is mapped `left_to_right`, and a plan that showed a field
  which would not actually land would be lying about the only thing it is read
  for.
- **`Ann.Lee@Example.com` and `ann.lee@example.com` matched.** Normalisation is
  not cosmetic: it is what identity matching compares, which is why it also
  refuses to fold plus-addressing by default.
- **The pair that disagrees is not resolved.** Both sides changed, the
  configured policy is `newest_wins`, and the two timestamps are inside the
  clock tolerance, so which one is newer is not knowable and nothing is
  written. Believing a millisecond there means whichever peer's clock drifts
  forward wins every conflict, for ever, invisibly.
- **`plan` exits 2 when it would change something**, so it can gate a
  deployment the way a diff does.

`plan` is not a second program that agrees with the first by coincidence. It is
the engine, over an overlay of the real state, with the writes recorded instead
of sent.

## Connectors

v1 targets two connectors that anyone can actually run:

- **HubSpot.** Free developer test accounts, and currently on date-based API
  versioning (`/crm/objects/2026-03/`), so the adapter has to deal with a real
  live migration rather than a frozen v3 snapshot.
- **Twenty.** Open source and self-hostable via Docker, with a REST API and
  webhooks, which means end-to-end tests can run in CI without a paid account.

A `fake` connector with fault injection backs the test suite.

## Roadmap

v1 scope, follow-up work and known non-goals are tracked in
[GitHub issues](https://github.com/mykolapodpriatov/crm-bisync/issues).

## License

MIT
