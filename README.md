# Pontmore

Reference implementation of the Pontmore protocol.

This repository is intentionally separate from the canonical protocol specifications in
[`pontmore/protocol`](https://github.com/pontmore/protocol). The specifications remain
authoritative when implementation behavior differs.

## Current scope

The executable slice provides:

- deterministic PIP-00 Agent-definition validation;
- deterministic PIP-01 escrow-descriptor validation and compatibility selection;
- signed PIP-02 version 2 root/action validation and deterministic `pontmore/swap@1`
  reconstruction over unordered event sets;
- authorization, deadline, dispute, finality, replay, and sibling-fork enforcement;
- stable machine-readable errors and language-neutral compatibility vectors;
- a PostgreSQL-backed `pontmored` with immutable signed-event storage, rebuildable
  coordination projections, durable connector-operation records, and an SSE event log;
- protocol-aware ingestion, automatic reconstruction, and idempotent execution of
  eligible escrow effects;
- durable leased work with multi-daemon `SKIP LOCKED` coordination and backoff;
- relay backfill/subscription/publication for kinds 30360, 30361, 7300, and 7301;
- safe, digest-pinned service-schema resolution with reviewed authority facts;
- validated signed outcomes with a reconciliation-capable publication outbox;
- static, digest-pinned, supervised long-lived economic connector subprocesses; and
- a `pontmore-tck` command that executes the compatibility vectors.

Support is **experimental**. A deployment must still supply authenticated ingress, an OS
sandbox launcher, reviewed schema policies, an isolated outcome-signing connector,
durable connector-side idempotency, and real economic connectors. The included connector
moves no value and does not sign outcomes.

### PIP-02 boundary

`validate_swap_coordination` accepts complete signed kind `7300` and `7301` events plus
a `DescriptorBinding`. The binding explicitly supplies PIP-01 currentness, lifetime,
network, and service-authority facts established outside the reducer. It does not make
an index or database record canonical protocol state.

`pontmored` resolves descriptors from configured relays, applies NIP-01 addressable
revision ordering at root and acceptance time, safely fetches service schemas, and pins
the artifact digest. Because PIP-01 delegates authority semantics to the schema, a local
`schema_policies` entry approves the digest, authority keys, and resolution effects.
Static `descriptor_bindings` remain an explicit offline/test fallback.

The reducer supports only PIP-02 content version 2 with the exact `pontmore/swap@1`
profile. Fork recovery, private commitment verification, descriptor retrieval, and
assessment of opaque payment-reference privacy remain outside this pure reconstruction
slice.

## Workspace

```text
crates/core/   package `pontmore`; deterministic protocol validation
crates/daemon/ package and binary `pontmored`; SSE node and connector manager
crates/tck/    package and binary `pontmore-tck`; compatibility-vector runner
tck/vectors/            language-neutral executable cases
```

## Run

```console
just format
just check

# Equivalent individual Cargo commands:
cargo test --workspace
cargo run -p pontmore-tck -- tck/vectors
cargo run -p pontmored -- --database-url postgres://localhost/pontmore
cargo clippy --workspace --all-targets -- -D warnings
```

## `pontmored`

The daemon defaults to `127.0.0.1:7300`. It verifies every signed event before durable
storage. Public signed events remain canonical; projections, stream cursors, connector
operations, and private database records are implementation overlays.

Start a local node with:

```console
docker compose up --build
curl http://127.0.0.1:7300/health/ready
curl -N http://127.0.0.1:7300/v1/stream
```

The deliberately small HTTP surface is:

- `POST /v1/events`: the only client-to-daemon data path; accepts a complete signed
  event as `{ "event": ... }` and returns only HTTP status (`202` inserted, `200`
  duplicate); and
- `GET /v1/stream`: the only daemon-to-client data path; emits an initial snapshot,
  durable replay, and live updates as Server-Sent Events.

`GET /health/live` and `GET /health/ready` are operational probes, not client data APIs.
Reconnect with `Last-Event-ID`; the daemon replays durable events strictly after that
sequence. A connection without a cursor receives a transactionally consistent snapshot
before live updates. Snapshots carry `schema_version: 1`. Expired cursors return `410`;
oversized replays return `409`; slow consumers receive a terminal `stream_error` and
must reconnect for a snapshot. Clients must still verify signed protocol events.
Connector-operation updates contain only operation ID, coordination ID, operation type,
and status. Requests, responses, idempotency keys, connector identities, and errors
remain private operator state.

The API has no hosted authentication layer and defaults to loopback. A network
deployment must put authenticated transport and authorization in front of it.

Daemon configuration uses one environment-variable namespace:

- `PONTMORE_DATABASE_URL` is required;
- `PONTMORE_LISTEN` defaults to `127.0.0.1:7300`;
- `PONTMORE_CONNECTOR_CONFIG` selects the static connector allowlist and descriptor
  registry; and
- `PONTMORE_CONNECTOR_TIMEOUT_MS` defaults to `30000`;
- `PONTMORE_RELAYS` is a comma-separated relay allowlist;
- `PONTMORE_RELAY_LOOKBACK_SECS` defaults to seven days;
- `PONTMORE_SCHEMA_MAX_BYTES` defaults to 2 MiB;
- `PONTMORE_WORK_LEASE_SECS` defaults to 60 and must exceed connector timeout;
- `PONTMORE_STREAM_MAX_REPLAY` defaults to 10,000 records;
- `PONTMORE_STREAM_RETENTION_SECS` defaults to seven days; and
- `PONTMORE_REQUIRE_CONNECTOR_SANDBOX=true` refuses unsandboxed connectors.

## Extensions and economic connectors

Pontmore has no protocol-plugin interface. PIP and coordination-profile behavior is
compiled into the core and changes only with a reviewed core release.

Application extensions are ordinary external clients. They submit complete signed events
through `POST /v1/events` and consume state through `GET /v1/stream`. They are never
loaded into `pontmored` and cannot call economic connectors.

Economic connectors are a separate internal boundary for narrowly typed effects such as
`escrow_secure`, `escrow_settle`, `escrow_refund`, and `resolve_dispute`. On startup the
daemon reads a static allowlist, verifies each executable digest and manifest, starts
one long-lived subprocess, clears its environment, and performs a versioned handshake.
Subsequent `health`, `execute`, `inspect`, and `shutdown` JSON Lines commands reuse that
process. Profile, network, connector type, value policy, and bound authority are checked
before dispatch. Connector success is only operational evidence. An outcome changes
protocol state only when the connector returns a signed action whose signature, signer,
root, predecessor, action, graph, and authority validate, and that event is published,
observed from a relay, ingested, and reconstructed.

Connector manifests also bind the protocol authority pubkeys they serve. A connector is
eligible only when type, exact profile, network, and root-bound authority all match. A
failed exchange discards the child; bounded backoff and a restart budget govern its
replacement. Periodic health and connector version/digest are durable. Every execute
command carries a deterministic idempotency key derived from the coordination,
triggering signed action, and operation type so the connector can enforce deduplication
independently of the daemon database.

Secrets are read from owner-restricted files only for manifest-declared operations and
sent through the private subprocess channel, never inherited through the environment or
included in durable requests/SSE. Connector stderr is bounded structured diagnostic
output. A configured sandbox launcher owns OS identity, resource, filesystem, syscall,
and network enforcement; production can require it fail closed.

An operation interrupted while `running` becomes `ambiguous` on daemon startup and must
be inspected before any retry. Startup performs that inspection before rebuilding stored
coordinations. The connector execution methods are internal and have no HTTP route.

For `pontmore/swap@1`, an applied `core/accept` creates an idempotent `escrow_secure`
operation, `core/authorize_settlement` creates `escrow_settle`, `core/authorize_refund`
creates `escrow_refund`, and `core/open_dispute` can dispatch a resolver. The daemon
never synthesizes an authority signature.

[`deploy/connectors.example.json`](deploy/connectors.example.json) enables the
deterministic no-value simulated escrow connector. Relative paths are resolved from the
configuration and manifest files. It is intentionally unsandboxed for portability. Read
[`docs/operations.md`](docs/operations.md) before enabling a value-bearing connector.

## Design rules

- Public signed protocol events are canonical; indexes and projections are not.
- Exact protocol and profile versions are selected explicitly.
- Validation is deterministic and independent of relay arrival order.
- Protocol semantics are not runtime-extensible.
- Application extensions remain outside the daemon.
- Economic connectors receive typed, daemon-authorized effects, never arbitrary client
  calls or raw database access.
- Private payment data, credentials, evidence, and operator judgments never become
  public protocol fields.

## Protocol baseline

The implemented rules are pinned to Pontmore protocol commit
`d9a1eb3e8a24e2101f5a4024df1ce85a8d934f75` dated 2026-09-14.

PIP-02 and `pontmore/swap@1` remain experimental. The implementation makes no
interoperability claim until an independently developed implementation runs the same
signed vectors successfully.

## License

Licensed under the MIT license.
