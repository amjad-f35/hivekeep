#!/usr/bin/env bash
# Lints the whole workspace, all targets, with warnings as errors.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
cargo clippy --workspace --all-targets --locked -- -D warnings
