# Image for the fspersist demo workload (REGISTRY_PLACEHOLDER/hivekeep-demo-fspersist).
# Build from the repository root:
#   docker build -f demos/multi-template/fspersist/Dockerfile -t REGISTRY_PLACEHOLDER/hivekeep-demo-fspersist:dev .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-demo-fspersist --bin fspersist \
  && install -D target/release/fspersist /out/fspersist

FROM debian:bookworm-slim
COPY --from=build /out/fspersist /app/fspersist
EXPOSE 80
ENTRYPOINT ["/app/fspersist"]
