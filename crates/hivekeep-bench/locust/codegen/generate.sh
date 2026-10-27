#!/usr/bin/env bash
# Generates the Python gRPC clients the Python user classes import
# (common/api_pb2.py, common/api_pb2_grpc.py, common/glutton_pb2.py, ...).
# They are build output, not sources: the locust image runs this at build
# time, and a local run of a Python class needs it once.
#
# Usage: codegen/generate.sh [PROTO...]   (default: the hivekeep-api and
#        glutton protos)
# Environment:
#   PYTHON    interpreter with grpcio-tools installed; when unset a venv
#             under codegen/venv is created from codegen/requirements.txt
#   OUT_DIR   output directory (default: locust/common)
set -o errexit -o nounset -o pipefail

LOCUST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "${LOCUST_DIR}/../../.." && pwd)"
cd "${ROOT}"

CODEGEN_DIR="${LOCUST_DIR}/codegen"
OUT_DIR="${OUT_DIR:-${LOCUST_DIR}/common}"

if [[ $# -eq 0 ]]; then
  set -- crates/hivekeep-proto/proto/hivekeep/api/v1/api.proto \
    crates/hivekeep-bench/proto/hivekeep/glutton/v1/glutton.proto
fi

if [[ -z "${PYTHON:-}" ]]; then
  # shellcheck source=../../../../scripts/lib/venv.sh
  . "${ROOT}/scripts/lib/venv.sh"
  hk_venv_ensure "${CODEGEN_DIR}/venv"
  hk_venv_install "${CODEGEN_DIR}/venv" "${CODEGEN_DIR}/requirements.txt"
  PYTHON="${CODEGEN_DIR}/venv/bin/python3"
fi

# python_proto PROTO_DIR BASE compiles PROTO_DIR/BASE.proto into OUT_DIR,
# flat: the generated modules are imported as common.<base>_pb2.
python_proto() {
  local proto_dir="$1" base="$2"
  "${PYTHON}" -m grpc_tools.protoc \
    -I"${proto_dir}" \
    --python_out="${OUT_DIR}/" \
    --grpc_python_out="${OUT_DIR}/" \
    "${proto_dir}/${base}.proto"

  # protoc writes `import <base>_pb2 as <base>__pb2`, which does not resolve
  # inside the common package. Rewritten through a temporary file, since GNU
  # and BSD sed disagree on -i.
  local grpc_file="${OUT_DIR}/${base}_pb2_grpc.py"
  sed "s/^import ${base}_pb2 as ${base}__pb2/from . import ${base}_pb2 as ${base}__pb2/" \
    "${grpc_file}" > "${grpc_file}.tmp"
  mv "${grpc_file}.tmp" "${grpc_file}"
}

mkdir -p "${OUT_DIR}"
echo "Generating Python gRPC clients into ${OUT_DIR}"
for proto in "$@"; do
  python_proto "$(dirname "${proto}")" "$(basename "${proto}" .proto)"
done
