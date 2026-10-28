#!/usr/bin/env bash
# Runs the Hivekeep installer (hivekeep-setup) with the given arguments, e.g.
#   scripts/install/hivekeep.sh deploy hivekeep-system
#   scripts/install/hivekeep.sh deploy demo counter
# The installer reads .hivekeep-dev-env.sh and the HK_* environment itself;
# this wrapper only builds and runs it. Set HK_SETUP_BIN to use a prebuilt
# binary instead of `cargo run --release`.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
hk_setup "$@"
