#!/usr/bin/env bash
# Runs the tunnel benchmark as three containers on one docker network:
#
#   hk-tunnel-worker  privileged; the tunnel in a cgroup of its own with the
#                     CPU and memory limits, hosting Actors with the worker's
#                     own code, plus the fake keeper and the ingress and
#                     egress actors
#   hk-tunnel-sink    the fake egress gateway
#   hk-tunnel-load    the load generator, in the foreground
#
# Arguments after the options go to `hivekeep-tunnel-bench load`:
#
#   crates/hivekeep-bench-tunnel/run-docker.sh --tunnel-cpus 1 --tunnel-memory 1g -- --rungs 1000,2000,4000
#
# Options:
#   --tunnel-cpus N     CPU limit for the tunnel (default 1)
#   --tunnel-memory M   memory limit for the tunnel, without swap (default 1g)
#   --actors N          Actors the tunnel hosts (default 10)
#   --cpuset-worker S   pin the worker container to these CPUs, e.g. 0-3
#   --cpuset-rig S      pin the generator and sink to these CPUs, e.g. 4-9
#
# Results land in crates/hivekeep-bench-tunnel/runs/<timestamp>/:
# results.txt, results.json and logs/.

set -o errexit -o nounset -o pipefail

tunnel_cpus=1
tunnel_memory=1g
actors=10
cpuset_worker=""
cpuset_rig=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tunnel-cpus) tunnel_cpus="$2"; shift 2 ;;
    --tunnel-memory) tunnel_memory="$2"; shift 2 ;;
    --actors) actors="$2"; shift 2 ;;
    --cpuset-worker) cpuset_worker="$2"; shift 2 ;;
    --cpuset-rig) cpuset_rig="$2"; shift 2 ;;
    --) shift; break ;;
    *) break ;;
  esac
done

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
out="${here}/runs/$(date +%Y%m%d-%H%M%S)"
# Under runs/ rather than /tmp so Docker Desktop can always mount it.
work="${here}/runs/.work-$$"
pki="${work}/pki"
mkdir -p "${pki}" "${out}/logs"
image=hivekeep-tunnel-bench:local
network=hk-tunnel-bench
# The egress and ingress actors serve on Unix sockets here. A named volume,
# because sockets do not work on Docker Desktop's host mounts.
shared=hk-tunnel-bench-shared
bin=hivekeep-tunnel-bench

echo "building ${image}"
docker build --quiet -t "${image}" -f "${here}/Dockerfile" "${repo}" >/dev/null

containers=(hk-tunnel-load hk-tunnel-worker hk-tunnel-sink)
remove_all() {
  for c in "${containers[@]}"; do
    docker rm -f "${c}" >/dev/null 2>&1 || true
  done
  docker network rm "${network}" >/dev/null 2>&1 || true
  docker volume rm "${shared}" >/dev/null 2>&1 || true
}
cleanup() {
  local code=$?
  # The worker logs every tunnel death, with the OOM kills its cgroup counted.
  if docker cp hk-tunnel-worker:/shared/tunnel-deaths.log "${out}/logs/" >/dev/null 2>&1; then
    local deaths ooms
    deaths="$(wc -l <"${out}/logs/tunnel-deaths.log" | tr -d ' ')"
    ooms="$(grep -c 'OOM kills: [1-9]' "${out}/logs/tunnel-deaths.log" || true)"
    echo "the tunnel died ${deaths} time(s), ${ooms} of them OOM-killed at its ${tunnel_memory} limit" | tee -a "${out}/results.txt"
  fi
  echo "run in ${out}"
  for c in "${containers[@]}"; do
    docker logs "${c}" >"${out}/logs/${c}.log" 2>&1 || true
  done
  remove_all
  rm -rf "${work}"
  exit "${code}"
}
trap cleanup EXIT
remove_all
docker network create "${network}" >/dev/null
docker volume create "${shared}" >/dev/null

# The generator and sink run as an unprivileged uid; it writes the PKI and
# the results.
chmod 0777 "${out}" "${pki}"
docker run --rm --user 65532 -v "${pki}:/pki" "${image}" \
  "${bin}" pki --dir /pki --worker-hosts hk-tunnel-worker --gateway-hosts hk-tunnel-sink

# The generator and the tunnel open many short connections; without these a
# run runs out of ephemeral ports to sockets in TIME_WAIT.
port_opts=(--sysctl "net.ipv4.ip_local_port_range=1024 65535" --sysctl net.ipv4.tcp_tw_reuse=1)
rig_opts=()
if [[ -n "${cpuset_rig}" ]]; then
  rig_opts+=(--cpuset-cpus "${cpuset_rig}")
fi
worker_opts=()
if [[ -n "${cpuset_worker}" ]]; then
  worker_opts+=(--cpuset-cpus "${cpuset_worker}")
fi

docker run --detach --network "${network}" --user 65532 ${rig_opts[@]+"${rig_opts[@]}"} --name hk-tunnel-sink \
  -v "${pki}:/pki:ro" "${image}" \
  "${bin}" sink --pki-dir /pki >/dev/null

# Privileged for the Actors' network namespaces, nftables and cgroups. The
# worker's credentials are mounted where a worker Pod's are projected, so the
# tunnel runs on its default flags.
docker run --detach --network "${network}" --privileged --user 0 --cgroupns private \
  "${port_opts[@]}" ${worker_opts[@]+"${worker_opts[@]}"} --name hk-tunnel-worker \
  -v "${pki}:/pki:ro" -v "${shared}:/shared" \
  -v "${pki}/worker.pem:/run/podidentity.podcert.hivekeep.io/credential-bundle.pem:ro" \
  -v "${pki}/ca.pem:/run/podidentity.podcert.hivekeep.io/trust-bundle.pem:ro" \
  -v "${pki}/ca.pem:/run/servicedns.podcert.hivekeep.io/trust-bundle.pem:ro" \
  "${image}" \
  "${bin}" worker --pki-dir /pki --shared-dir /shared \
  --tunnel-cpus "${tunnel_cpus}" --tunnel-memory "${tunnel_memory}" -- \
  --actors "${actors}" --gateway hk-tunnel-sink:9443 --dns-egress-policy enforce-in-tunnel >/dev/null

# The generator runs in the foreground; Ctrl-C stops it and the trap cleans up.
docker run --network "${network}" --user 65532 "${port_opts[@]}" ${rig_opts[@]+"${rig_opts[@]}"} --name hk-tunnel-load \
  -v "${pki}:/pki:ro" -v "${shared}:/shared" -v "${out}:/out" "${image}" \
  "${bin}" load --pki-dir /pki --shared-dir /shared \
  --results-json /out/results.json "$@" | tee "${out}/results.txt"
