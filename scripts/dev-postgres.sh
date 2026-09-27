#!/usr/bin/env bash
# Run a throwaway PostgreSQL cluster from the Nix dev shell for local tests.
set -euo pipefail

command=${1:?usage: dev-postgres.sh start|stop|status}
data_dir=${PONTMORE_DEV_PG_DIR:-.pontmore/postgres}
port=${PONTMORE_DEV_PG_PORT:-55432}
user=pontmore

case "${command}" in
  start)
    if [[ ! -f "${data_dir}/PG_VERSION" ]]; then
      mkdir -p "${data_dir}"
      initdb --pgdata "${data_dir}" --username "${user}" --auth trust >/dev/null
    fi
    if ! pg_ctl --pgdata "${data_dir}" status >/dev/null 2>&1; then
      data_path=$(cd "${data_dir}" && pwd)
      pg_ctl --pgdata "${data_dir}" --log "${data_dir}/postgres.log" --wait \
        -o "-p ${port} -k ${data_path} -c listen_addresses=127.0.0.1" start >/dev/null
    fi
    for database in pontmore pontmore_test; do
      if ! psql -h 127.0.0.1 -p "${port}" -U "${user}" -d postgres -tAc \
        "SELECT 1 FROM pg_database WHERE datname = '${database}'" | grep -q 1; then
        createdb -h 127.0.0.1 -p "${port}" -U "${user}" "${database}"
      fi
    done
    printf 'PostgreSQL is listening on 127.0.0.1:%s\n' "${port}"
    printf 'PONTMORE_DATABASE_URL=postgres://%s@127.0.0.1:%s/pontmore\n' "${user}" "${port}"
    printf 'PONTMORE_TEST_DATABASE_URL=postgres://%s@127.0.0.1:%s/pontmore_test\n' "${user}" "${port}"
    ;;
  stop)
    if [[ -f "${data_dir}/PG_VERSION" ]]; then
      pg_ctl --pgdata "${data_dir}" --mode fast stop >/dev/null 2>&1 || true
    fi
    ;;
  status)
    pg_ctl --pgdata "${data_dir}" status
    ;;
  *)
    printf 'unknown command: %s\n' "${command}" >&2
    exit 2
    ;;
esac
