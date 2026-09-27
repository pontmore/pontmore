#!/usr/bin/env bash
root="$(git rev-parse --show-toplevel)"
dot_git="$(git rev-parse --git-common-dir)"
if [[ ! -d "${dot_git}/hooks" ]]; then mkdir -p "${dot_git}/hooks"; fi
# fix old bug
if [[ -e "${dot_git}/hooks/pre-comit" || -L "${dot_git}/hooks/pre-comit" ]]; then
  rm -f "${dot_git}/hooks/pre-comit"
fi
hook="${dot_git}/hooks/pre-commit"
source="${root}/misc/git-hooks/pre-commit"
if [[ ! -e "${hook}" ]] || ! cmp -s "${source}" "${hook}"; then
  rm -f "${hook}"
  ln -sf "${source}" "${hook}"
fi

if ! flakebox lint --silent; then
  >&2 echo "ℹ️  Project recommendations detected. Run 'flakebox lint' for more info."
fi
