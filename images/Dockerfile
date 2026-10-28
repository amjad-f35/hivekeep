# syntax=docker/dockerfile:1.10
#
# One Dockerfile for every Hivekeep component image. hivekeep-setup builds it
# from the repository root:
#
#   docker buildx build -f images/Dockerfile \
#     --platform=linux/amd64,linux/arm64 \
#     --build-arg=COMPONENT=hivekeep-api \
#     --build-arg=VERSION=v0.1.0 --build-arg=COMMIT=<sha> --build-arg=BUILD_DATE=<rfc3339> \
#     -t <repo>/hivekeep-api:<tag> --push .
#
# COMPONENT names the image (one of hivekeep-setup's images::COMPONENTS) and
# selects both the binary compiled in the build stage and the runtime stage
# the image ends in. The compiler always runs on the build host's platform and
# cross-compiles for TARGETARCH, so a multi-platform build never runs rustc
# under emulation.

ARG COMPONENT
ARG RUST_IMAGE=rust:1.98.1-bookworm
ARG RUNTIME_NONROOT=gcr.io/distroless/cc-debian12:nonroot
ARG RUNTIME_ROOT=gcr.io/distroless/cc-debian12
ARG RUNTIME_SHELL=debian:bookworm-slim

# ---------------------------------------------------------------------------
# Build: compile the one binary COMPONENT ships.
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM ${RUST_IMAGE} AS build
ARG COMPONENT
ARG TARGETARCH
ARG BUILDARCH
# protoc release used on the build host.
ARG PROTOC_VERSION=29.3

# protoc compiles the gRPC definitions (the definitions use options newer than
# Debian's packaged protoc, so a pinned release runs on the build host);
# cmake and clang serve the C parts of
# the TLS and crypto backends. The cross toolchain is only installed when the
# target architecture differs from the build host's.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    set -eu; \
    rm -f /etc/apt/apt.conf.d/docker-clean; \
    pkgs="unzip curl ca-certificates cmake clang"; \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      case "$TARGETARCH" in \
        amd64) pkgs="$pkgs gcc-x86-64-linux-gnu g++-x86-64-linux-gnu libc6-dev-amd64-cross" ;; \
        arm64) pkgs="$pkgs gcc-aarch64-linux-gnu g++-aarch64-linux-gnu libc6-dev-arm64-cross" ;; \
        *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
      esac; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends $pkgs; \
    case "$BUILDARCH" in \
      amd64) parch=x86_64 ;; \
      arm64) parch=aarch_64 ;; \
      *) echo "unsupported BUILDARCH $BUILDARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/protoc.zip \
      "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${parch}.zip"; \
    unzip -q -o /tmp/protoc.zip -d /usr/local bin/protoc 'include/*'; \
    rm /tmp/protoc.zip; \
    protoc --version

RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    rustup target add "$triple"; \
    echo "$triple" > /rust-target

# Linkers and C compilers per target triple. On a native build these name the
# host compiler, which Debian also installs under its triplet name.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ \
    AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar \
    PROTOC=/usr/local/bin/protoc \
    CARGO_TERM_COLOR=never

WORKDIR /src
COPY . .

# COMPONENT -> <crate> <binary>. Keep in step with images::COMPONENTS in
# crates/hivekeep-setup/src/images/mod.rs; an unknown name fails the build.
# The target directory lives in a cache mount, so the binary is copied out to
# /out within the same step. Demo ActorTemplates start their binary from
# /app; the gVisor worker reads its runsc flag table from HK_DATA_PATH.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    set -eu; \
    case "$COMPONENT" in \
      hivekeep-api)                     crate=hivekeep-api;                 bin=hivekeep-api ;; \
      hivekeep-controller)              crate=hivekeep-controller;          bin=hivekeep-controller ;; \
      hivekeep-credential-provider-k8s) crate=hivekeep-credential-provider; bin=hivekeep-credential-provider ;; \
      hivekeep-demo-counter)            crate=hivekeep-demo-counter;        bin=counter ;; \
      hivekeep-demo-egress)             crate=hivekeep-demo-egress;         bin=egress ;; \
      hivekeep-demo-fspersist)          crate=hivekeep-demo-fspersist;      bin=fspersist ;; \
      hivekeep-demo-sandbox)            crate=hivekeep-demo-sandbox;        bin=sandbox ;; \
      hivekeep-fake-keeper)             crate=hivekeep-bench;               bin=fake-keeper ;; \
      hivekeep-fake-workersync)         crate=hivekeep-bench;               bin=fake-workersync ;; \
      hivekeep-gate)                    crate=hivekeep-gate;                bin=hivekeep-gate ;; \
      hivekeep-idp-server)              crate=hivekeep-idp;                 bin=hivekeep-idp ;; \
      hivekeep-keeper)                  crate=hivekeep-keeper;              bin=hivekeep-keeper ;; \
      hivekeep-objectstore-plugin)      crate=hivekeep-objectstore;         bin=hivekeep-objectstore-plugin ;; \
      hivekeep-podcert-controller)      crate=hivekeep-podcert-controller;  bin=hivekeep-podcert-controller ;; \
      hivekeep-sandbox-gvisor)          crate=hivekeep-sandbox-gvisor;      bin=hivekeep-sandbox-gvisor ;; \
      hivekeep-sandbox-microvm)         crate=hivekeep-sandbox-microvm;     bin=hivekeep-sandbox-microvm ;; \
      "") echo "set --build-arg=COMPONENT" >&2; exit 1 ;; \
      *) echo "unknown COMPONENT $COMPONENT" >&2; exit 1 ;; \
    esac; \
    triple="$(cat /rust-target)"; \
    cargo build --release --locked --target "$triple" -p "$crate" --bin "$bin"; \
    mkdir -p /out/usr/local/bin; \
    cp "target/$triple/release/$bin" /out/usr/local/bin/; \
    case "$COMPONENT" in \
      hivekeep-demo-*) mkdir -p /out/app; cp "target/$triple/release/$bin" /out/app/ ;; \
      hivekeep-sandbox-gvisor) mkdir -p /out/usr/share/hivekeep; \
        cp crates/hivekeep-sandbox-gvisor/data/internal_flags.json /out/usr/share/hivekeep/ ;; \
    esac

# ---------------------------------------------------------------------------
# Runtime bases. Every image carries the same OCI labels.
# ---------------------------------------------------------------------------
FROM ${RUNTIME_NONROOT} AS runtime-nonroot
ARG COMPONENT
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE
LABEL org.opencontainers.image.title="${COMPONENT}" \
      org.opencontainers.image.vendor="Hivekeep" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"
COPY --from=build /out/ /

# Node components that need root on the host: keeper and the sandbox workers.
FROM ${RUNTIME_ROOT} AS runtime-root
ARG COMPONENT
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE
LABEL org.opencontainers.image.title="${COMPONENT}" \
      org.opencontainers.image.vendor="Hivekeep" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"
COPY --from=build /out/ /

# The sandbox demo runs commands it is sent, so it keeps a shell and coreutils.
FROM ${RUNTIME_SHELL} AS runtime-shell
ARG COMPONENT
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE
LABEL org.opencontainers.image.title="${COMPONENT}" \
      org.opencontainers.image.vendor="Hivekeep" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /

# ---------------------------------------------------------------------------
# One stage per component; the last line picks the one COMPONENT names.
# ---------------------------------------------------------------------------
FROM runtime-nonroot AS hivekeep-api
ENTRYPOINT ["/usr/local/bin/hivekeep-api"]

FROM runtime-nonroot AS hivekeep-controller
ENTRYPOINT ["/usr/local/bin/hivekeep-controller"]

FROM runtime-nonroot AS hivekeep-credential-provider-k8s
ENTRYPOINT ["/usr/local/bin/hivekeep-credential-provider"]

FROM runtime-nonroot AS hivekeep-demo-counter
ENTRYPOINT ["/app/counter"]

FROM runtime-nonroot AS hivekeep-demo-egress
ENTRYPOINT ["/app/egress"]

FROM runtime-nonroot AS hivekeep-demo-fspersist
ENTRYPOINT ["/app/fspersist"]

FROM runtime-shell AS hivekeep-demo-sandbox
ENTRYPOINT ["/app/sandbox"]

FROM runtime-nonroot AS hivekeep-fake-keeper
ENTRYPOINT ["/usr/local/bin/fake-keeper"]

FROM runtime-nonroot AS hivekeep-fake-workersync
ENTRYPOINT ["/usr/local/bin/fake-workersync"]

FROM runtime-nonroot AS hivekeep-gate
ENTRYPOINT ["/usr/local/bin/hivekeep-gate"]

FROM runtime-nonroot AS hivekeep-idp-server
ENTRYPOINT ["/usr/local/bin/hivekeep-idp"]

FROM runtime-root AS hivekeep-keeper
ENTRYPOINT ["/usr/local/bin/hivekeep-keeper"]

# Runs as root beside keeper (the pod sets runAsUser: 0) and as non-root
# beside hivekeep-api; the image defaults to non-root.
FROM runtime-nonroot AS hivekeep-objectstore-plugin
ENTRYPOINT ["/usr/local/bin/hivekeep-objectstore-plugin"]

FROM runtime-nonroot AS hivekeep-podcert-controller
ENTRYPOINT ["/usr/local/bin/hivekeep-podcert-controller"]

FROM runtime-root AS hivekeep-sandbox-gvisor
ENV HK_DATA_PATH=/usr/share/hivekeep
ENTRYPOINT ["/usr/local/bin/hivekeep-sandbox-gvisor"]

FROM runtime-root AS hivekeep-sandbox-microvm
ENTRYPOINT ["/usr/local/bin/hivekeep-sandbox-microvm"]

FROM ${COMPONENT}
