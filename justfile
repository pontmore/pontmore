set shell := ["bash", "-euc"]
set unstable := true

default: check

# Format the justfile, Rust workspace, documentation, fixtures, workflows, and manifests.
format:
    just --fmt
    cargo fmt --all
    npm run format:files

# Verify formatting without modifying files.
format-check:
    just --fmt --check
    cargo fmt --all -- --check
    npm run format:files:check

# Run Clippy across all packages and targets.
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Run the complete workspace test suite.
test:
    cargo test --workspace --locked

# Execute all language-neutral compatibility vectors.
tck:
    cargo run --locked -p pontmore-tck -- tck/vectors

# Run every non-mutating repository check.
check: format-check lint test tck
