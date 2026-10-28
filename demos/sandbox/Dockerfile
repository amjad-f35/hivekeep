# Image for the sandbox demo workload (REGISTRY_PLACEHOLDER/hivekeep-demo-sandbox).
# The runtime is Alpine so commands sent to the sandbox have a small,
# familiar userland. Build from the repository root:
#   docker build -f demos/sandbox/Dockerfile -t REGISTRY_PLACEHOLDER/hivekeep-demo-sandbox:dev .

FROM rust:1.98-alpine AS build
RUN apk add --no-cache musl-dev protobuf-dev protoc
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hivekeep-demo-sandbox --bin sandbox \
  && install -D target/release/sandbox /out/sandbox

FROM alpine:3.22
COPY --from=build /out/sandbox /app/sandbox
ENV PORT=80
EXPOSE 80
WORKDIR /root
ENTRYPOINT ["/app/sandbox"]
