#!/usr/bin/env bash
# Build, load, and publish the Nix-built pontmored OCI image.
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  container-image.sh build                  build the image archive for this system
  container-image.sh load [TAG]             build and load it into Docker (default pontmored:local)
  container-image.sh publish IMAGE TAG...   push IMAGE:TAG-ARCH for this system's architecture
  container-image.sh manifest IMAGE TAG     combine IMAGE:TAG-amd64 and IMAGE:TAG-arm64 into IMAGE:TAG

Registry credentials come from `docker login`, `skopeo login`, or REGISTRY_AUTH_FILE.
EOF
  exit 2
}

build() {
  nix build --print-build-logs --out-link result-container .#container.pontmored
  readlink -f result-container
}

architecture() {
  case "$(nix eval --raw --impure --expr builtins.currentSystem)" in
    x86_64-linux) echo amd64 ;;
    aarch64-linux) echo arm64 ;;
    *)
      echo "container images are only built on x86_64-linux and aarch64-linux" >&2
      exit 1
      ;;
  esac
}

command=${1:-}
shift || true
case "${command}" in
  build)
    build
    ;;
  load)
    target=${1:-pontmored:local}
    archive=$(build)
    loaded=$(docker load --quiet <"${archive}" | awk '/Loaded image:/ { print $3 }')
    docker tag "${loaded}" "${target}"
    echo "${target}"
    ;;
  publish)
    [[ $# -ge 2 ]] || usage
    image=$1
    shift
    archive=$(build)
    arch=$(architecture)
    for tag in "$@"; do
      skopeo copy --retry-times 3 "docker-archive:${archive}" "docker://${image}:${tag}-${arch}"
    done
    ;;
  manifest)
    [[ $# -eq 2 ]] || usage
    manifest-tool push from-args \
      --platforms linux/amd64,linux/arm64 \
      --template "$1:$2-ARCH" \
      --target "$1:$2"
    ;;
  *)
    usage
    ;;
esac
