# Pontmore

Reference implementation and production-framework workspace for the Pontmore
protocol family.

This repository is intentionally separate from the canonical protocol
specifications in [`pontmore/protocol`](https://github.com/pontmore/protocol).
The specifications remain authoritative when implementation behavior differs.

## Current scope

The first executable slice provides:

- deterministic validation of PIP-00 Agent-definition content and indexing tags;
- deterministic validation of PIP-01 escrow-descriptor content, indexing tags,
  mechanism/network compatibility, and caller-supplied selection context;
- stable machine-readable validation error codes;
- language-neutral JSON compatibility vectors; and
- a `pontmore-tck` command that executes those vectors.

Support is **experimental**. This first slice does not yet implement:

- Nostr event ID or signature verification;
- relay synchronization or addressable-event revision discovery;
- DNS-aware service-schema fetch safety;
- PIP-02 coordination reconstruction or `pontmore/swap@1`;
- persistence, signing, plugins, or economic side effects.

Those omissions are hard validation boundaries, not implied conformance.

## Workspace

```text
crates/pontmore-core/   deterministic protocol validation
crates/pontmore-tck/    compatibility-vector runner
tck/vectors/            language-neutral executable cases
```

## Run

```console
cargo test --workspace
cargo run -p pontmore-tck -- tck/vectors
cargo clippy --workspace --all-targets -- -D warnings
```

## Design rules

- Public signed protocol events are canonical; indexes and projections are not.
- Exact protocol and profile versions are selected explicitly.
- Validation remains deterministic and independent of relay arrival order.
- Infrastructure and service connectors may become pluggable; protocol safety
  invariants do not.
- Private payment data, credentials, evidence, and operator judgments never
  become public protocol fields.

## Protocol baseline

Development started from Pontmore protocol commit
`d9a1eb3e8a24e2101f5a4024df1ce85a8d934f75` dated 2026-09-14.

## License

Licensed under the MIT license.
