# Production operations

`pontmored` keeps signed Pontmore events canonical. PostgreSQL queues, checkpoints,
projections, connector records, and stream cursors are implementation overlays.

## Required controls

- Put authenticated TLS and per-operator authorization in front of the loopback HTTP
  listener. The current daemon is single-operator, so SSE needs no tenant filtering.
- Configure multiple independently operated relays where practical. Readiness remains
  false until every configured relay completes its initial backfill.
- Set `PONTMORE_REQUIRE_CONNECTOR_SANDBOX=true` for value-bearing deployments. Each
  connector must then name an operator-reviewed launcher. That launcher must enforce a
  dedicated unprivileged identity, read-only mounts, private temporary storage,
  process/CPU/memory ceilings, syscall restrictions, and network denial or an explicit
  proxy allowlist. Arguments are passed literally and never interpolated.
- Store connector secrets in owner-only files and map only manifest-declared names.
  Values are loaded for that operation and sent over stdin, not inherited via
  environment.
- Set a finite `execution_policy.max_automatic_sats`. Higher-value coordinations require
  an explicit `approved_coordination_ids` entry.
- Use connectors that durably deduplicate the supplied idempotency key independently of
  the daemon database and return complete signed outcome events.

## Failure semantics

Workers use PostgreSQL row locks and expiring leases. Retryable work uses bounded
exponential delay; deadlines and attempt limits end retries. A crash during connector
dispatch marks the operation ambiguous at restart, then calls `inspect` before deciding
anything further. Settlement and refund publications have a database uniqueness guard in
addition to PIP-02 finality validation.

Relay publication is an outbox. Pending and stale-publishing rows are retried;
successful sends are queried back; only an observed signed event is ingested. Connector
success or a lost relay acknowledgement cannot directly advance canonical state.

## Upgrades and shutdown

Migrations are additive: `0001` and `0002` installations upgrade through `0003`. The
first upgraded start records an execution activation boundary so historical backfill
reconstructs state without dispatching old effects. Events received after activation
remain eligible across restarts.

SIGINT or SIGTERM stops HTTP acceptance, signals relay/worker/retention/health tasks,
waits for bounded operations, asks connectors to shut down, and exits.
