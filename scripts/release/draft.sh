#!/usr/bin/env bash
# Creates a DRAFT GitHub release for a tag, with generated notes plus a list
# of everyone who committed since the previous final release. A maintainer
# reviews and publishes the draft.
#
#   scripts/release/draft.sh [--dry-run] <vMAJOR.MINOR.PATCH[-PRE]>
#
# --dry-run prints the notes and creates nothing; the tag need not exist yet
# (notes then run up to origin/main). An existing release is never touched.
#
# Needs git, gh (authenticated with write access; the notes API demands it
# even for a dry run). Environment:
#   GITHUB_REPOSITORY   owner/name of the repository (required)
#   HK_RELEASE_IMAGE_REPO  registry path release images are published under;
#                          when set, the notes gain install instructions
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
hk_require_cmd git gh

REPO="${GITHUB_REPOSITORY:?set GITHUB_REPOSITORY to owner/name}"
SEMVER='^v[0-9]+\.[0-9]+\.[0-9]+$'
SEMVER_PRE='^v[0-9]+\.[0-9]+\.[0-9]+-[0-9A-Za-z.-]+$'

errfile="$(mktemp)"
trap 'rm -f "${errfile}"' EXIT

api() {
  local what="$1"
  shift
  if ! gh api "$@" 2>"${errfile}"; then
    echo "error: ${what}: $(<"${errfile}")" >&2
    return 1
  fi
}

# exists returns 1 only for "not found" (404, or 422 for an unknown commit);
# any other failure stops the script so it is not read as absence.
exists() {
  if gh api "$1" --silent 2>"${errfile}"; then
    return 0
  fi
  grep -qE 'HTTP (404|422)' "${errfile}" && return 1
  hk_die "checking $1: $(<"${errfile}")"
}

dry_run=false
if [[ "${1:-}" == --dry-run ]]; then
  dry_run=true
  shift
fi
[[ $# -eq 1 ]] || hk_die "usage: $0 [--dry-run] <tag>"
tag="$1"

prerelease=false
if [[ "${tag}" =~ ${SEMVER_PRE} ]]; then
  prerelease=true
elif ! [[ "${tag}" =~ ${SEMVER} ]]; then
  hk_die "${tag} is not a vMAJOR.MINOR.PATCH[-PRERELEASE] tag"
fi

target="${tag}"
if ! exists "repos/${REPO}/git/ref/tags/${tag}"; then
  [[ "${dry_run}" == true ]] || hk_die "tag ${tag} is not in ${REPO}; push it first or use --dry-run"
  target="$(git rev-parse remotes/origin/main)"
  exists "repos/${REPO}/commits/${target}" || hk_die "origin/main (${target}) is not in ${REPO}"
fi

# Notes start at the newest final release below this version, so every
# release candidate of a version shares one starting point.
base="${tag%%-*}"
tags="$(api "listing tags" "repos/${REPO}/tags" --paginate --jq '.[].name')"
grep -qE "${SEMVER}" <<<"${tags}" || hk_die "${REPO} has no vX.Y.Z tags to start the notes from"
previous="$(
  {
    grep -E "${SEMVER}" <<<"${tags}" | grep -vxF "${base}" || true
    echo "${base}"
  } | sort -V | grep -B1 -xF "${base}" | grep -vxF "${base}" || true
)"

args=(-f "tag_name=${tag}")
[[ "${target}" != "${tag}" ]] && args+=(-f "target_commitish=${target}")
[[ -n "${previous}" ]] && args+=(-f "previous_tag_name=${previous}")
notes="$(api "generating notes" "repos/${REPO}/releases/generate-notes" "${args[@]}" --jq .body)" ||
  hk_die "could not generate notes (a 404 here usually means the token lacks write access)"

if [[ -n "${HK_RELEASE_IMAGE_REPO:-}" ]]; then
  notes+=$'\n\n## Images\n\n'
  notes+="Images are published as \`${HK_RELEASE_IMAGE_REPO}/<image>:${tag}\` for \`linux/amd64\` and \`linux/arm64\`."
  notes+=$'\n\n## Install\n\nFrom a checkout of this tag:\n\n```sh\n'
  notes+="cargo run --release -p hivekeep-setup -- deploy hivekeep-system \\"$'\n'
  notes+="  --image-repo ${HK_RELEASE_IMAGE_REPO} --image-tag ${tag} \\"$'\n'
  notes+=$'  --credential-provider=\'{"name":"k8s.io"}\' \\\n'
  notes+=$'  --ingress-allowed-spiffe-ids=<SPIFFE IDs of your ingress clients>\n```'
  notes+=$'\n\n## hivectl\n\n'
  notes+="Binaries for Linux and macOS (amd64, arm64) are attached with \`.sha256\` files:"
  notes+=$'\n\n```sh\nshasum -a 256 -c hivectl-linux-amd64.sha256\ninstall -m 0755 hivectl-linux-amd64 ~/.local/bin/hivectl\n```'
fi

if [[ -n "${previous}" ]] && git rev-parse -q --verify "${previous}^{commit}" >/dev/null; then
  committers="$(git log --format='%aN' "${previous}..${target}" | sort -fu | sed 's/^/- /')"
  if [[ -n "${committers}" ]]; then
    notes+=$'\n\n## Committers in this release\n\n'"${committers}"
  fi
fi

echo "Release notes for ${tag} (previous release: ${previous:-none}):"
echo
echo "${notes}"
echo
if [[ "${dry_run}" == true ]]; then
  echo "Dry run: no release created."
  exit 0
fi
if gh release view "${tag}" --repo "${REPO}" >/dev/null 2>&1; then
  echo "A release for ${tag} already exists; left unchanged."
  exit 0
fi
create=(--repo "${REPO}" --draft --verify-tag --title "${tag}" --notes-file -)
[[ "${prerelease}" == true ]] && create+=(--prerelease)
gh release create "${tag}" "${create[@]}" <<<"${notes}"
