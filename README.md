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
- static, digest-pinned, long-lived economic connector subprocesses; and
- a `pontmore-tck` command that executes the compatibility vectors.

Support is **experimental**. This slice does not yet provide relay synchronization,
signing, automatic coordination reconstruction on event intake, descriptor discovery,
real economic connectors, connector restart supervision, or OS-level connector
sandboxing. Those omissions are hard implementation boundaries, not implied conformance.

### PIP-02 boundary

`validate_swap_coordination` accepts complete signed kind `7300` and `7301` events plus
a `DescriptorBinding`. The binding explicitly supplies PIP-01 currentness, lifetime,
network, and service-authority facts established outside the reducer. It does not make
an index or database record canonical protocol state.

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
before live updates. Clients must still verify signed protocol events themselves.

The API has no hosted authentication layer and defaults to loopback. A network
deployment must put authenticated transport and authorization in front of it.

Daemon configuration uses one environment-variable namespace:

- `PONTMORE_DATABASE_URL` is required;
- `PONTMORE_LISTEN` defaults to `127.0.0.1:7300`;
- `PONTMORE_CONNECTOR_CONFIG` selects the static connector allowlist; and
- `PONTMORE_CONNECTOR_TIMEOUT_MS` defaults to `30000`.

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
process. Profile, network, and connector type are checked before dispatch. Connector
responses are operational evidence only; they cannot directly create canonical Pontmore
state.

An operation interrupted while `running` becomes `ambiguous` on daemon startup and must
be inspected before any retry. The connector execution methods are internal and have no
HTTP route.

[`deploy/connectors.example.json`](deploy/connectors.example.json) enables the
deterministic no-value simulated escrow connector. Relative paths are resolved from the
configuration and manifest files. Until OS sandboxing is added, operators must treat
configured executables as trusted code.

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
