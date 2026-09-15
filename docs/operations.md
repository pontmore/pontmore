# Production operations

`pontmored` keeps signed Pontmore events canonical. PostgreSQL queues, checkpoints,
projections, connector records, and stream cursors are implementation overlays.

## Required controls

- Keep the client API and operator UI as separate network planes. The client API is the
  `PONTMORE_LISTEN` listener; the operator UI is the `PONTMORE_UI_LISTEN` listener. Do
  not expose the UI as a Pontmore client interface.
- Keep the operator UI on loopback when using its default passwordless mode. A
  non-loopback UI bind fails closed unless `PONTMORE_UI_PASSWORD_FILE` and
  `PONTMORE_UI_SECURE_COOKIES=true` are both configured. Terminate authenticated TLS in
  front of that listener, restrict ingress to operators, and store the password in a
  regular owner-only file (mode `0600` on Unix). The current UI is single-operator.
- Put authenticated transport and authorization in front of the client API before a
  non-loopback deployment. The UI's login is not client API authentication.
- Configure multiple independently operated relays where practical. Startup relays come
  from `PONTMORE_RELAYS`; operators can add durable relays from Configuration while the
  daemon is running. Remote operator URLs must use `wss://` (`ws://` is accepted only
  for loopback development). Readiness remains false until every effective relay
  completes backfill and has a recent connection heartbeat. A saved but unreachable
  relay therefore makes readiness false until it connects. The UI shows connection,
  backfill, and error state; it does not alter signed Nostr events.
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

SIGINT or SIGTERM stops both HTTP listeners, signals relay/worker/retention/health
tasks, waits for bounded operations, asks connectors to shut down, and exits. Operator
sessions are intentionally invalidated on every restart because the in-memory signing
key is not persisted.

## Operator UI security

The UI queries and changes implementation state through the daemon's existing storage,
coordinator, and connector abstractions; it does not treat database projections as
canonical protocol state. Operator actions can submit an already-signed event, pause or
resume new economic execution, queue projection rebuilds, retry failed work or
publications, probe connector health, and request graceful shutdown. Every modifying
form requires the authenticated session's CSRF token. Browser responses set a
restrictive content-security policy, deny framing and content sniffing, and reject
cross-origin form submissions.

The economic execution switch is durable and shared by daemons using the same database.
Pausing does not cancel a connector exchange already in flight. Resuming requeues every
stored coordination; durable operation and connector idempotency remain responsible for
preventing repeated value movement. Operator-added relay URLs are stored in a separate
database table and rediscovered by the relay worker; other startup-managed configuration
is deliberately not rewritten from the browser.

The embedded UI has no CDN or runtime package dependency. Its authenticated
`GET /ui/stream` connection refreshes on daemon events and periodically polls the
storage view for operational changes. Do not route `/ui/stream` to Pontmore clients;
their stable stream remains `GET /v1/stream` on the client listener.
