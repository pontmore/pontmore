set shell := ["bash", "-euc"]
set unstable := true

default: check

# Install all contributor dependencies after cloning.
setup: setup-rust setup-format

# Fetch the locked Rust crates (the pinned toolchain is selected by rustup).
setup-rust:
    cargo fetch --locked

# Install the locked, development-only JavaScript formatters.
setup-format:
    npm ci

# Start the local Docker stack and keep its PostgreSQL data between runs.
docker-up:
    bash scripts/docker-up.sh

# Stop only the local Docker stack; the PostgreSQL volume is preserved.
docker-down:
    docker compose -p pontmore-local down

# Show the local Docker services and their health.
docker-status:
    docker compose -p pontmore-local ps

# Follow logs from the local Docker daemon.
docker-logs:
    docker compose -p pontmore-local logs -f pontmored

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

# Start pontmored; PONTMORE_DATABASE_URL is required.
daemon *args:
    cargo run --locked -p pontmored -- {{ args }}

# Verify graceful whole-daemon restart and SSE snapshot recovery.
restart-smoke database_url:
    cargo build --locked -p pontmored
    bash scripts/restart-smoke.sh {{ quote(database_url) }}

# Run every non-mutating repository check.
check: format-check lint test tck
