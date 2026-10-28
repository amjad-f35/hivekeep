# syntax=docker/dockerfile:1.10
#
# The Envoy image the gate's envoy router and egress gateway run
# (hivekeep-setup's images::ENVOY_DATAPLANE, pushed as
# <repo>/hivekeep-envoy-dataplane). hivekeep-setup builds it with this
# directory as the context:
#
#   docker buildx build --platform=linux/amd64,linux/arm64 \
#     -t <repo>/hivekeep-envoy-dataplane:<tag> --push images/envoy-dataplane
#
# What the manifests rely on (gate-router.yaml, gate-egress.yaml):
#   - /usr/local/bin/envoy, and envoy as the entrypoint: the router sets
#     `command`, the egress gateway passes only `args`
#   - the bootstrap at /etc/envoy/envoy.yaml, mounted from a ConfigMap
#   - `sh` and `sleep` for the preStop drain hook
#   - runs as UID 65532 (the pods set runAsUser; the image works as any
#     non-root UID)
#   - the egress-policy dynamic module, loaded by name
#     (envoy_hivekeep_egress_policy) from ENVOY_DYNAMIC_MODULES_SEARCH_PATH
#
# The module is cross-compiled on the build host, so a multi-platform build
# never runs rustc under emulation.

ARG RUST_IMAGE=rust:1.98.1-bookworm
# Multi-platform index, so --platform picks the matching Envoy binary. The
# SDK revision in dynamic-modules/Cargo.toml must not be newer than this.
ARG ENVOY_IMAGE=envoyproxy/envoy:v1.39.5@sha256:8fc594a5d85eb76dca24155cfb9208882406873932b20cf33649a0a611194c43

FROM --platform=$BUILDPLATFORM ${RUST_IMAGE} AS build
ARG BUILDARCH
ARG TARGETARCH
WORKDIR /src
# bindgen (in the SDK build) needs libclang; a cross build also needs the
# target's gcc and libc.
RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64) arch=x86_64 ;; \
      arm64) arch=aarch64 ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    echo "${arch}" >/target-arch; \
    cross=""; \
    if [ "${BUILDARCH}" != "${TARGETARCH}" ]; then \
      cross="gcc-$(echo "${arch}" | tr _ -)-linux-gnu libc6-dev-${TARGETARCH}-cross"; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends clang libclang-dev ${cross}; \
    rm -rf /var/lib/apt/lists/*; \
    rustup target add "${arch}-unknown-linux-gnu"
COPY dynamic-modules/ .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    set -eu; \
    arch="$(cat /target-arch)"; \
    triple="${arch}-unknown-linux-gnu"; \
    if [ "${BUILDARCH}" != "${TARGETARCH}" ]; then \
      export "CARGO_TARGET_$(echo "${arch}" | tr a-z A-Z)_UNKNOWN_LINUX_GNU_LINKER=${arch}-linux-gnu-gcc"; \
    fi; \
    locked=""; \
    if [ -f Cargo.lock ]; then locked=--locked; fi; \
    cargo build --release ${locked} --target "${triple}" -p hivekeep-envoy-egress-policy; \
    install -D -m 0644 "target/${triple}/release/libenvoy_hivekeep_egress_policy.so" \
      /out/libenvoy_hivekeep_egress_policy.so

FROM ${ENVOY_IMAGE}
COPY --from=build /out/libenvoy_hivekeep_egress_policy.so /usr/local/lib/
ENV ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/envoy"]
CMD ["-c", "/etc/envoy/envoy.yaml"]
