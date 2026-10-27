# Image for the counter demo workload (REGISTRY_PLACEHOLDER/hivekeep-demo-counter).
# Build from the repository root:
#   docker build -f demos/counter/Dockerfile -t REGISTRY_PLACEHOLDER/hivekeep-demo-counter:dev .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-demo-counter --bin counter \
  && install -D target/release/counter /out/counter

FROM debian:bookworm-slim
COPY --from=build /out/counter /app/counter
EXPOSE 80
ENTRYPOINT ["/app/counter"]
