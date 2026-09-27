#!/usr/bin/env bash
# Print the image tags for the current GitHub Actions ref.
set -euo pipefail

short_sha=${GITHUB_SHA:?GITHUB_SHA is required}
short_sha=${short_sha:0:12}

case "${GITHUB_REF_TYPE:-}:${GITHUB_REF_NAME:-}" in
  tag:v*)
    echo "${GITHUB_REF_NAME}"
    if [[ "${GITHUB_REF_NAME}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      echo latest
    fi
    ;;
  branch:main)
    echo main
    echo "sha-${short_sha}"
    ;;
  *)
    echo "sha-${short_sha}"
    ;;
esac
