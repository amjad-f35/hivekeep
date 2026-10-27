# One image holding the tunnel benchmark's one program; each container picks
# its subcommand. The worker container also needs `ip` and `nft`, which set up
# the Actors' sandbox networks. Build from the repository root (run-docker.sh
# does this):
#   docker build -f crates/hivekeep-bench-tunnel/Dockerfile -t hivekeep-tunnel-bench:local .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-bench-tunnel --bin hivekeep-tunnel-bench \
  && install -D target/release/hivekeep-tunnel-bench /out/hivekeep-tunnel-bench

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates iproute2 nftables \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/hivekeep-tunnel-bench /usr/local/bin/hivekeep-tunnel-bench
