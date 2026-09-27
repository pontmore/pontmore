set shell := ["bash", "-euc"]
set unstable

default: check

# Fetch the locked Rust crates. Run inside `nix develop`, which provides every tool.
setup:
    cargo fetch --locked

# Run every Nix flake check: toolchain pin, clippy, tests, TCK, and image.
nix-check:
    nix flake check --print-build-logs

# Start the local PostgreSQL cluster used by tests and `just daemon`.
db-up:
    bash scripts/dev-postgres.sh start

# Stop the local PostgreSQL cluster; its data under .pontmore/postgres is kept.
db-down:
    bash scripts/dev-postgres.sh stop

# Build the pontmored OCI image archive with Nix.
image:
    bash scripts/container-image.sh build

# Build the pontmored OCI image and load it into Docker.
image-load tag="pontmored:local":
    bash scripts/container-image.sh load {{ quote(tag) }}

# Push the image for this architecture as IMAGE:TAG-ARCH for each tag.
image-publish image +tags:
    bash scripts/container-image.sh publish {{ quote(image) }} {{ tags }}

# Combine the amd64 and arm64 images into the multi-architecture IMAGE:TAG.
image-manifest image tag:
    bash scripts/container-image.sh manifest {{ quote(image) }} {{ quote(tag) }}

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
    nixfmt $(git ls-files '*.nix')
    npm run format:files

# Verify formatting without modifying files.
format-check:
    just --fmt --check
    cargo fmt --all -- --check
    nixfmt --check $(git ls-files '*.nix')
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
