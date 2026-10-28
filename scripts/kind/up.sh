#!/usr/bin/env bash
# Creates a Kind cluster for Hivekeep development, plus a local image
# registry the nodes pull from.
#
# Environment:
#   KIND_CLUSTER_NAME      cluster name (default: kind)
#   KIND_REGISTRY_PORT     host port of the local registry (default: 5001)
#   IP_FAMILY              ipv4 | ipv6 | dual (default: ipv4)
#   HK_KIND_RESERVED_CPU   CPU reserved per node for kubelet and system (default: none)
#   HK_KIND_NODE_IMAGE     node image override (default: kind's own)
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
CONTEXT="kind-${KIND_CLUSTER_NAME}"
REGISTRY_NAME="${HK_KIND_REGISTRY_NAME:-kind-registry}"
REGISTRY_PORT="${KIND_REGISTRY_PORT:-5001}"
REGISTRY_IMAGE="${HK_KIND_REGISTRY_IMAGE:-registry:3}"
IP_FAMILY="${IP_FAMILY:-ipv4}"

if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
  sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

case "${IP_FAMILY}" in
  ipv4 | ipv6 | dual) ;;
  *) hk_die "IP_FAMILY must be ipv4, ipv6 or dual (got '${IP_FAMILY}')" ;;
esac
hk_require_cmd docker kubectl

work="${HK_ROOT}/bin"
mkdir -p "${work}"
config="${work}/kind-config.yaml"

registry_running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "${REGISTRY_NAME}" 2>/dev/null || true)" == true ]]
}

# 1. Local registry, recreated if it is bound to another port.
hk_log "local registry ${REGISTRY_NAME} on 127.0.0.1:${REGISTRY_PORT}"
if registry_running && ! docker port "${REGISTRY_NAME}" | grep -q ":${REGISTRY_PORT}\$"; then
  hk_log "registry is mapped to another port; recreating it"
  docker rm -f "${REGISTRY_NAME}" >/dev/null
fi
if ! registry_running; then
  docker rm -f "${REGISTRY_NAME}" >/dev/null 2>&1 || true
  docker run -d --restart=always \
    --label created-by=hivekeep \
    -p "127.0.0.1:${REGISTRY_PORT}:5000" \
    -p "[::1]:${REGISTRY_PORT}:5000" \
    --network bridge --name "${REGISTRY_NAME}" \
    "${REGISTRY_IMAGE}" >/dev/null
fi

# 2. KVM where the nodes will run (inside the Docker VM on macOS). Without it
# the cluster still runs gVisor workers; micro-VM workers need it.
has_kvm=false
if docker run --rm --device /dev/kvm busybox true >/dev/null 2>&1; then
  has_kvm=true
  hk_log "/dev/kvm is available: micro-VM workers will be schedulable"
else
  hk_log "/dev/kvm is not available: only gVisor workers will run"
fi

# 3. Cluster config. Pod certificates and ClusterTrustBundles are behind
# feature gates; image pulls are parallel so the install's third-party images
# don't queue behind one another on a single node.
{
  cat <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
YAML
  if [[ -n "${HK_KIND_NODE_IMAGE:-}" ]]; then
    echo "  image: ${HK_KIND_NODE_IMAGE}"
  fi
  if [[ "${has_kvm}" == true ]]; then
    cat <<YAML
  extraMounts:
  - hostPath: /dev/kvm
    containerPath: /dev/kvm
YAML
  fi
  cat <<YAML
featureGates:
  ClusterTrustBundle: true
  ClusterTrustBundleProjection: true
  PodCertificateRequest: true
runtimeConfig:
  "certificates.k8s.io/v1beta1": "true"
networking:
  ipFamily: ${IP_FAMILY}
kubeadmConfigPatches:
- |
  kind: KubeletConfiguration
  serializeImagePulls: false
  maxParallelImagePulls: 4
YAML
  # Many sandboxes on one node can starve the kubelet heartbeat; reserving
  # CPU for it keeps the node from being marked NotReady.
  if [[ -n "${HK_KIND_RESERVED_CPU:-}" ]]; then
    cat <<YAML
  systemReserved:
    cpu: "${HK_KIND_RESERVED_CPU}"
  kubeReserved:
    cpu: "${HK_KIND_RESERVED_CPU}"
YAML
  fi
} >"${config}"

hk_log "replacing any existing cluster ${KIND_CLUSTER_NAME}"
hk_kind delete cluster --name "${KIND_CLUSTER_NAME}" >/dev/null 2>&1 || true

# The "kind" Docker network outlives clusters; one created without IPv6 leaves
# v6 nodes without addresses, so rebuild it for non-ipv4 clusters.
if [[ "${IP_FAMILY}" != ipv4 &&
  "$(docker network inspect kind --format '{{.EnableIPv6}}' 2>/dev/null || echo absent)" == false ]]; then
  hk_log "recreating the kind Docker network with IPv6"
  docker network disconnect kind "${REGISTRY_NAME}" 2>/dev/null || true
  docker network rm kind >/dev/null ||
    hk_die "the kind Docker network is still in use; disconnect its containers and retry"
fi

hk_log "creating cluster ${KIND_CLUSTER_NAME} (ipFamily=${IP_FAMILY})"
hk_kind create cluster --name "${KIND_CLUSTER_NAME}" --config "${config}"

if [[ "${IP_FAMILY}" != ipv4 && "$(docker network inspect kind --format '{{.EnableIPv6}}')" != true ]]; then
  hk_die "Docker gave the kind network no IPv6; enable ipv6 and ip6tables in the daemon config and retry"
fi

# kind publishes an ipv6 API server on [::1], which a VM-hosted Docker daemon
# does not forward; localhost is on the serving cert and works either way.
if [[ "${IP_FAMILY}" == ipv6 ]]; then
  server="$(kubectl config view -o jsonpath="{.clusters[?(@.name==\"${CONTEXT}\")].cluster.server}")"
  if [[ "${server}" == "https://[::1]:"* ]]; then
    kubectl config set-cluster "${CONTEXT}" --server="https://localhost:${server##*:}" >/dev/null
  fi
fi

# The neighbour table limit is global across network namespaces; each sandbox
# takes an entry, so the default overflows around a thousand sandboxes.
if thresh="$(sysctl -n net.ipv4.neigh.default.gc_thresh3 2>/dev/null)" && [[ "${thresh}" -le 1024 ]]; then
  hk_warn "net.ipv4.neigh.default.gc_thresh3=${thresh}; for more than ~1000 sandboxes raise gc_thresh1/2/3 to 4096/16384/32768"
fi

nodes="$(hk_kind get nodes --name "${KIND_CLUSTER_NAME}")"
for node in ${nodes}; do
  # gVisor sandboxes reach each other through the node; proxy ARP/NDP lets the
  # node answer for them. -e tolerates kernels without IPv6.
  docker exec "${node}" sysctl -q net.ipv4.conf.all.proxy_arp=1
  docker exec "${node}" sysctl -q -e net.ipv6.conf.all.proxy_ndp=1
  if [[ "${has_kvm}" == true ]]; then
    docker exec "${node}" chmod 666 /dev/kvm
  fi
  # Point containerd at the local registry.
  dir="/etc/containerd/certs.d/localhost:${REGISTRY_PORT}"
  docker exec "${node}" mkdir -p "${dir}"
  printf '[host."http://%s:5000"]\n' "${REGISTRY_NAME}" | docker exec -i "${node}" cp /dev/stdin "${dir}/hosts.toml"
done

if [[ "$(docker inspect -f '{{json .NetworkSettings.Networks.kind}}' "${REGISTRY_NAME}")" == null ]]; then
  docker network connect kind "${REGISTRY_NAME}"
fi

# Advertise the registry the way local-registry-aware tools expect.
kubectl --context="${CONTEXT}" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${REGISTRY_PORT}"
YAML
hk_log "cluster ready: kubectl context ${CONTEXT}, registry localhost:${REGISTRY_PORT}"
