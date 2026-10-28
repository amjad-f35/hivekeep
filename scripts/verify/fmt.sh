#!/usr/bin/env bash
# Fails when any Rust source is not rustfmt-formatted (scripts/update/fmt.sh fixes it).
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
cargo fmt --all -- --check
