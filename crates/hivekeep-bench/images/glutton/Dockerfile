# The glutton benchmark workload (REGISTRY_PLACEHOLDER/hivekeep-bench-glutton).
# Build from the repository root:
#   docker build -f crates/hivekeep-bench/images/glutton/Dockerfile \
#     -t REGISTRY_PLACEHOLDER/hivekeep-bench-glutton:dev .

FROM rust:1.98-slim-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-bench --bin glutton \
  && install -D target/release/glutton /out/glutton

FROM debian:bookworm-slim
COPY --from=build /out/glutton /app/glutton
# Port 80 is the port the worker forwards into the sandbox; the template
# passes --grpc-listen-addr=:80 --mode=http.
EXPOSE 80 9090
ENTRYPOINT ["/app/glutton"]
