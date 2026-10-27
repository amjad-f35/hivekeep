# The locust benchmark image (REGISTRY_PLACEHOLDER/hivekeep-bench-locust):
# the locust master with its locustfiles, the headless runner, and the
# locust-worker load generator. A manifest picks which one a container runs.
# Build from the repository root (locust/build_and_push.sh does):
#   docker build -f crates/hivekeep-bench/locust/Dockerfile \
#     -t REGISTRY_PLACEHOLDER/hivekeep-bench-locust:dev .

# Python dependencies, for the runtime image's interpreter.
FROM python:3.11-slim AS pydeps
WORKDIR /app
COPY crates/hivekeep-bench/locust/requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

# The Python gRPC clients, generated from the protos (codegen/generate.sh).
FROM python:3.11-slim AS pyprotos
WORKDIR /src
COPY crates/hivekeep-bench/locust/codegen/requirements.txt crates/hivekeep-bench/locust/codegen/
RUN pip install --no-cache-dir -r crates/hivekeep-bench/locust/codegen/requirements.txt
COPY crates/hivekeep-bench/locust/codegen/generate.sh crates/hivekeep-bench/locust/codegen/
COPY crates/hivekeep-proto/proto crates/hivekeep-proto/proto
COPY crates/hivekeep-bench/proto crates/hivekeep-bench/proto
RUN PYTHON=python3 OUT_DIR=/out crates/hivekeep-bench/locust/codegen/generate.sh

# The load generator.
FROM rust:1.98-slim-bookworm AS worker
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-bench --bin locust-worker \
  && install -D target/release/locust-worker /out/locust-worker

FROM python:3.11-slim
RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin locust
WORKDIR /app
COPY --from=pydeps /app/deps /app/deps
COPY --from=worker /out/locust-worker /app/locust-worker
COPY crates/hivekeep-bench/locust/common/ /app/common/
COPY --from=pyprotos /out/ /app/common/
COPY crates/hivekeep-bench/locust/shapes/ /app/shapes/
COPY crates/hivekeep-bench/locust/tests/ /app/tests/
COPY crates/hivekeep-bench/locust/runner.py crates/hivekeep-bench/locust/cluster_facts.py \
     crates/hivekeep-bench/locust/server_telemetry.py crates/hivekeep-bench/locust/phase_breakdown.py \
     crates/hivekeep-bench/locust/ladder_steps.py /app/
COPY crates/hivekeep-bench/analysis/phase_report.py /app/phase_report.py

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
USER 65532
# The entrypoint is the interpreter: the master runs `-m locust ...`, a
# headless run `/app/runner.py ...`; the worker container overrides the
# command with /app/locust-worker.
ENTRYPOINT ["python3"]
