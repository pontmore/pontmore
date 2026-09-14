#!/usr/bin/env bash
set -euo pipefail

database_url=${1:?usage: restart-smoke.sh DATABASE_URL}
listen=${PONTMORE_RESTART_TEST_LISTEN:-127.0.0.1:17300}
base_url="http://${listen}"
scratch=$(mktemp -d)
daemon_pid=

cleanup() {
  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill -TERM "${daemon_pid}"
    wait "${daemon_pid}" || true
  fi
  rm -rf "${scratch}"
}
trap cleanup EXIT

start_daemon() {
  PONTMORE_DATABASE_URL="${database_url}" PONTMORE_LISTEN="${listen}" \
    target/debug/pontmored >"${scratch}/daemon.log" 2>&1 &
  daemon_pid=$!
  for _ in $(seq 1 100); do
    if curl --fail --silent "${base_url}/health/ready" >/dev/null; then
      return
    fi
    sleep 0.05
  done
  cat "${scratch}/daemon.log"
  return 1
}

assert_snapshot() {
  timeout 2 curl --silent --no-buffer "${base_url}/v1/stream" >"${scratch}/stream" || true
  grep -q '^event: snapshot' "${scratch}/stream"
  grep -q '"schema_version":1' "${scratch}/stream"
}

start_daemon
assert_snapshot
kill -TERM "${daemon_pid}"
wait "${daemon_pid}"
daemon_pid=
start_daemon
assert_snapshot
