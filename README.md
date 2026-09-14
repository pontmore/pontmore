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
- signed PIP-02 version 2 root/action validation and deterministic
  `pontmore/swap@1` reconstruction over unordered event sets;
- authorization, deadline, dispute, finality, replay, and sibling-fork
  enforcement for the experimental swap profile;
- stable machine-readable validation error codes;
- language-neutral JSON compatibility vectors; and
- a `pontmore-tck` command that executes those vectors.

Support is **experimental**. This first slice does not yet implement:

- relay synchronization or addressable-event revision discovery;
- DNS-aware service-schema fetch safety;
- persistence, signing, plugins, or economic side effects.

Those omissions are hard validation boundaries, not implied conformance.

### PIP-02 boundary

`validate_swap_coordination` accepts complete signed kind `7300` and `7301`
events plus a `DescriptorBinding`. The binding is an explicit input containing
PIP-01 currentness, lifetime, network, and service-authority facts established
outside the reducer. Supplying it does not turn an index or database record into
canonical protocol state.

The reducer currently supports only PIP-02 content version 2 with the exact
`pontmore/swap@1` profile. It rejects other versions and profiles. Fork recovery,
private commitment payload verification, descriptor retrieval, and evaluation
of whether an opaque payment reference leaks private data remain outside this
pure reconstruction slice.

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

The implemented rules are pinned to Pontmore protocol commit
`d9a1eb3e8a24e2101f5a4024df1ce85a8d934f75` dated 2026-09-14.

PIP-02 and `pontmore/swap@1` remain experimental. The implementation makes no
interoperability claim until an independently developed implementation runs the
same signed vectors successfully.

## License

Licensed under the MIT license.
