# The egress benchmark (REGISTRY_PLACEHOLDER/hivekeep-bench-egress): one
# binary whose `actor` subcommand runs inside the sandbox and whose `target`
# subcommand serves the endpoints behind the egress gateway. Build from the
# repository root:
#   docker build -f crates/hivekeep-bench/images/egress-bench/Dockerfile \
#     -t REGISTRY_PLACEHOLDER/hivekeep-bench-egress:dev .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-bench --bin egress-bench \
  && install -D target/release/egress-bench /out/egress-bench

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/egress-bench /app/egress-bench
EXPOSE 80 8080
ENTRYPOINT ["/app/egress-bench"]
CMD ["actor"]
