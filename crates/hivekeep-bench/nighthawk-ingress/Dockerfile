# The Nighthawk gate router capacity runner image. Build context: the
# repository root, always for linux/amd64 (the nighthawk-dev images are
# amd64 only):
#   docker build --platform linux/amd64 -f crates/hivekeep-bench/nighthawk-ingress/Dockerfile .

# These three move together: NIGHTHAWK_IMAGE_DIGEST is the digest of the
# nighthawk-dev image built at NIGHTHAWK_COMMIT, and ENVOY_COMMIT is the Envoy
# revision Nighthawk pins at that commit.
ARG NIGHTHAWK_COMMIT=21f3f100b86b11e5211e5672ade956c87baaa75c
ARG ENVOY_COMMIT=ad01ae36a702169140334ebadb2d0fc3be56327e
ARG NIGHTHAWK_IMAGE_DIGEST=sha256:7729902afea9d83f593b240690ba7d6e8fff04f6215ada538cfb4c1452b2553e

# --- Stage 1: Nighthawk descriptor set --------------------------------------
# spec.py builds the session spec and output.py reads the result through the
# real Nighthawk message types, which pull in Envoy, xds, validate and
# googleapis types. This stage compiles that closure into one
# FileDescriptorSet.
FROM python:3.12-slim AS protogen
ARG NIGHTHAWK_COMMIT
ARG ENVOY_COMMIT
COPY crates/hivekeep-bench/nighthawk-ingress/protogen/requirements.txt /tmp/protogen-requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r /tmp/protogen-requirements.txt
# Shallow fetches; Nighthawk and Envoy are pinned to bare commits, which
# `clone --branch` cannot fetch.
RUN git clone --depth 1 https://github.com/cncf/xds /src/xds \
    && git clone --depth 1 https://github.com/bufbuild/protoc-gen-validate /src/pgv \
    && git clone --depth 1 https://github.com/googleapis/googleapis /src/googleapis
RUN git init /src/nighthawk \
    && git -C /src/nighthawk fetch --depth 1 https://github.com/envoyproxy/nighthawk "${NIGHTHAWK_COMMIT}" \
    && git -C /src/nighthawk checkout FETCH_HEAD
RUN git init /src/envoy \
    && git -C /src/envoy fetch --depth 1 https://github.com/envoyproxy/envoy "${ENVOY_COMMIT}" \
    && git -C /src/envoy checkout FETCH_HEAD
COPY crates/hivekeep-bench/nighthawk-ingress/protogen/generate_descriptor.sh /gen.sh
RUN bash /gen.sh /out/nighthawk.desc

# --- Stage 2: hivekeep.api.v1 Python stubs ----------------------------------
# Generated from the workspace's protos rather than checked in. Only the
# control API: this benchmark never talks to glutton directly.
FROM python:3.12-slim AS apistubs
COPY crates/hivekeep-bench/nighthawk-ingress/protogen/requirements.txt /tmp/protogen-requirements.txt
RUN pip install --no-cache-dir -r /tmp/protogen-requirements.txt
COPY crates/hivekeep-proto/proto /proto
RUN mkdir /out && python3 -m grpc_tools.protoc -I/proto \
        --python_out=/out --grpc_python_out=/out hivekeep/api/v1/api.proto

# --- Stage 3: runtime --------------------------------------------------------
# nighthawk-dev has no stable tags, hence the digest. It ships the
# nighthawk_* binaries in /usr/local/bin.
FROM envoyproxy/nighthawk-dev@${NIGHTHAWK_IMAGE_DIGEST}

# Installed with this image's own interpreter: grpcio and protobuf have
# version-specific native extensions.
COPY crates/hivekeep-bench/nighthawk-ingress/requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && python3 -m pip install --no-cache-dir --break-system-packages --target=/app/deps -r /tmp/requirements.txt \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

WORKDIR /app
COPY --from=protogen /out/nighthawk.desc /app/nighthawk.desc
COPY --from=apistubs /out/ /app/
COPY crates/hivekeep-bench/nighthawk-ingress/runner.py \
     crates/hivekeep-bench/nighthawk-ingress/spec.py \
     crates/hivekeep-bench/nighthawk-ingress/output.py \
     crates/hivekeep-bench/nighthawk-ingress/actors.py \
     /app/
COPY crates/hivekeep-bench/nighthawk-ingress/tests /app/tests

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
ENV NIGHTHAWK_DESC=/app/nighthawk.desc

ENTRYPOINT ["python3", "/app/runner.py"]
