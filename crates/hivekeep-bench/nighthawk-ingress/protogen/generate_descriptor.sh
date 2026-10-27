#!/usr/bin/env bash
# Compiles the Nighthawk protos the runner needs (adaptive load, client and
# request source, with everything they import) into one FileDescriptorSet,
# so the image carries a single file instead of the proto trees.
#
# The source trees are cloned by the Dockerfile's protogen stage:
#   /src/nighthawk   envoyproxy/nighthawk at NIGHTHAWK_COMMIT
#   /src/envoy       envoyproxy/envoy at Nighthawk's pinned ENVOY_COMMIT
#   /src/xds         cncf/xds (the xds/ and udpa/ import roots)
#   /src/pgv         bufbuild/protoc-gen-validate
#   /src/googleapis  googleapis/googleapis
set -euo pipefail

OUT="${1:-/out/nighthawk.desc}"
mkdir -p "$(dirname "${OUT}")"
cd /src/nighthawk

# shellcheck disable=SC2046 # the globs are meant to split
python3 -m grpc_tools.protoc \
  --include_imports \
  --descriptor_set_out="${OUT}" \
  -I/src/nighthawk \
  -I/src/envoy/api \
  -I/src/xds \
  -I/src/pgv \
  -I/src/googleapis \
  api/adaptive_load/*.proto \
  api/client/*.proto \
  api/request_source/*.proto

echo "wrote ${OUT} ($(wc -c <"${OUT}") bytes)"
