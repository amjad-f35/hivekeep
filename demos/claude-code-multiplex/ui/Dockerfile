# Dashboard image for the multiplex demo, built from the workspace root:
#
#   docker build -f demos/claude-code-multiplex/ui/Dockerfile \
#     -t REGISTRY_PLACEHOLDER/hivekeep-demo-multiplex-ui:0.1.0 .
#
# The page (index.html) is compiled into the binary, so the context must be
# the workspace root.
FROM rust:1.98-slim AS build
WORKDIR /src
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
ENV PROTOC=/usr/bin/protoc
COPY . .
RUN cargo build --release --locked -p hivekeep-demo-multiplex-ui --bin multiplex-ui

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/multiplex-ui /app/multiplex-ui
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/app/multiplex-ui"]
