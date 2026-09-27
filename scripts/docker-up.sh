#!/usr/bin/env bash
set -euo pipefail

env_file=.env
password_dir=.pontmore
password_file="${password_dir}/ui-password"

if [[ -L "${env_file}" || ( -e "${env_file}" && ! -f "${env_file}" ) ]]; then
  printf 'Refusing non-regular local environment path: %s\n' "${env_file}" >&2
  exit 1
fi
if [[ ! -f "${env_file}" ]]; then
  cp .env.example "${env_file}"
  printf 'Created %s from .env.example; edit it for local settings.\n' "${env_file}"
fi
chmod 600 "${env_file}"

if [[ -L "${password_dir}" || -L "${password_file}" || ( -e "${password_file}" && ! -f "${password_file}" ) ]]; then
  printf 'Refusing non-regular operator password path: %s\n' "${password_file}" >&2
  exit 1
fi

if [[ ! -f "${password_file}" ]]; then
  mkdir -p "${password_dir}"
  umask 077
  openssl rand -hex 32 >"${password_file}"
  printf 'Generated local operator password in %s\n' "${password_file}"
fi

chmod 600 "${password_file}"

export PONTMORE_IMAGE=${PONTMORE_IMAGE:-pontmored:local}
bash scripts/container-image.sh load "${PONTMORE_IMAGE}" >/dev/null
docker compose -p pontmore-local up --detach --wait
printf 'Local Docker stack is running. Ports are configured in %s.\n' "${env_file}"
printf 'Operator UI password is in %s.\n' "${password_file}"
