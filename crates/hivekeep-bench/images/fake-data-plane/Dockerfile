# The fake data plane: fake-keeper (REGISTRY_PLACEHOLDER/hivekeep-fake-keeper)
# and fake-workersync (REGISTRY_PLACEHOLDER/hivekeep-fake-workersync), one
# build, one final stage per binary. Build from the repository root:
#   docker build -f crates/hivekeep-bench/images/fake-data-plane/Dockerfile \
#     --target fake-keeper -t REGISTRY_PLACEHOLDER/hivekeep-fake-keeper:dev .
#   docker build -f crates/hivekeep-bench/images/fake-data-plane/Dockerfile \
#     --target fake-workersync -t REGISTRY_PLACEHOLDER/hivekeep-fake-workersync:dev .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-bench --bin fake-keeper --bin fake-workersync \
  && install -D target/release/fake-keeper /out/fake-keeper \
  && install -D target/release/fake-workersync /out/fake-workersync

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

FROM runtime AS fake-workersync
COPY --from=build /out/fake-workersync /app/fake-workersync
ENTRYPOINT ["/app/fake-workersync"]

# Last, so a build without --target yields fake-keeper.
FROM runtime AS fake-keeper
COPY --from=build /out/fake-keeper /app/fake-keeper
ENTRYPOINT ["/app/fake-keeper"]
