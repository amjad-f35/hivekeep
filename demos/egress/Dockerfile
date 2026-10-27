# Image for the egress demo workload (REGISTRY_PLACEHOLDER/hivekeep-demo-egress).
# Build from the repository root:
#   docker build -f demos/egress/Dockerfile -t REGISTRY_PLACEHOLDER/hivekeep-demo-egress:dev .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-demo-egress --bin egress \
  && install -D target/release/egress /out/egress

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/egress /app/egress
EXPOSE 80
ENTRYPOINT ["/app/egress"]
