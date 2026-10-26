# The benchmark orchestrator image. Build context: this directory.
#   docker build --platform=linux/amd64 -t <image> crates/hivekeep-bench/automation/

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        gettext-base \
        git \
        gnupg \
        jq \
        make \
        pkg-config \
        protobuf-compiler \
        python3 \
        python3-pip \
        python3-venv \
        docker.io \
        docker-buildx \
        unzip \
        zstd \
    && rm -rf /var/lib/apt/lists/*

# The installer and hivectl are built from the cloned workspace, so the image
# carries a Rust toolchain at the workspace's minimum version.
ARG RUST_VERSION=1.98.0
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}"

# gcloud, kubectl and the GKE auth plugin.
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        google-cloud-cli \
        google-cloud-cli-gke-gcloud-auth-plugin \
        kubectl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/automation

COPY requirements.txt /opt/automation/requirements.txt
RUN python3 -m venv /opt/automation/venv \
 && /opt/automation/venv/bin/pip install --no-cache-dir -r /opt/automation/requirements.txt
ENV PATH=/opt/automation/venv/bin:$PATH

COPY orchestrator.py util.py controlplane.py /opt/automation/
COPY testtypes/ /opt/automation/testtypes/
COPY manifests/runner-job.yaml.tmpl manifests/nighthawk-ingress-runner-job.yaml.tmpl /opt/automation/manifests/

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "/opt/automation/orchestrator.py"]
