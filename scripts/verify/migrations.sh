#!/usr/bin/env bash
# Checks the PostgreSQL migrations hivekeep-store embeds:
#   * files are NNNN_name.sql, numbered 1..n without gaps;
#   * none manages its own transaction (sqlx wraps each one) or hides
#     drift behind IF NOT EXISTS.
# With a base ref (scripts/verify/migrations.sh origin/main), also fails if a
# migration that exists at the base was modified or deleted.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
export LC_ALL=C

dir="${HK_MIGRATIONS_DIR:-crates/hivekeep-store/migrations}"
(( $# <= 1 )) || hk_die "usage: $0 [base-ref]"

shopt -s nullglob
migrations=("${dir}"/*.sql)
(( ${#migrations[@]} > 0 )) || hk_die "no migrations in ${dir}"

expected=1
for m in "${migrations[@]}"; do
  name="${m##*/}"
  [[ "${name}" =~ ^([0-9]{4})_[a-z0-9_]+\.sql$ ]] || hk_die "${name} must be named NNNN_name.sql"
  version=$((10#${BASH_REMATCH[1]}))
  (( version == expected )) || hk_die "$(printf 'expected migration %04d, found %s' "${expected}" "${name}")"
  if grep -Fiq -- 'IF NOT EXISTS' "${m}"; then
    hk_die "${name} must not use IF NOT EXISTS"
  fi
  if grep -Eiq -- '^[[:space:]]*(BEGIN|START[[:space:]]+TRANSACTION|COMMIT|ROLLBACK)[[:space:]]*;' "${m}"; then
    hk_die "${name} must leave transaction control to the migrator"
  fi
  expected=$((expected + 1))
done

if (( $# == 1 )); then
  hk_require_cmd git
  base="$(git merge-base HEAD "$1")"
  if ! git diff --quiet --no-renames --diff-filter=MD "${base}" HEAD -- "${dir}"; then
    echo "merged migrations must not change or disappear:" >&2
    git diff --name-only --no-renames --diff-filter=MD "${base}" HEAD -- "${dir}" >&2
    exit 1
  fi
fi
