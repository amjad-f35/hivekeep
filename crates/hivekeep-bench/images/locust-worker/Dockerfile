# The locust load-generator worker (REGISTRY_PLACEHOLDER/hivekeep-bench-locust-worker).
# It joins a stock locust master as a regular worker; the master runs from
# its own upstream image (see crates/hivekeep-bench/locust/manifests/locust.yaml).
# Build from the repository root:
#   docker build -f crates/hivekeep-bench/images/locust-worker/Dockerfile \
#     -t REGISTRY_PLACEHOLDER/hivekeep-bench-locust-worker:dev .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-bench --bin locust-worker \
  && install -D target/release/locust-worker /out/locust-worker

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/locust-worker /app/locust-worker
EXPOSE 8001
ENTRYPOINT ["/app/locust-worker"]
