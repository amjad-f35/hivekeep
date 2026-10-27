#!/usr/bin/env bash
# Oversubscription load for the request-parking demo.
#
# Runs one request-then-suspend loop per Actor against the 2-worker parking
# pool. With more Actors than workers the pool stays saturated; each loop's
# suspend frees a worker for a competitor, standing in for an Actor going
# idle. The tally shows parking turning momentary saturation into slower 200s
# instead of 503s. Compare a run with the router's default parking settings
# against one with --parked-request-max=0 (see README.md).
#
#   demos/parking/load.sh [-d seconds] [-H hive] [actor ...]
#     -d  duration (default 30)
#     -H  hive of the Actors (default hivekeep-demo-parking)
#     actors default to p1 p2 p3 p4
#
# Needs hivectl on PATH (or HIVECTL), and an install whose ingress allowlist
# includes spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client.
set -euo pipefail

DURATION=30
HIVE=hivekeep-demo-parking
TEMPLATE=parking
BASE_PORT="${BASE_PORT:-18000}"
HIVECTL="${HIVECTL:-hivectl}"

while getopts ":d:H:h" opt; do
  case "${opt}" in
    d) DURATION="${OPTARG}" ;;
    H) HIVE="${OPTARG}" ;;
    h) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option -${OPTARG}; use -h" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
ACTORS=("$@")
[[ ${#ACTORS[@]} -gt 0 ]] || ACTORS=(p1 p2 p3 p4)

tmp="$(mktemp -d)"
loops=()
proxies=()
cleanup() {
  if [[ ${#proxies[@]} -gt 0 ]]; then kill "${proxies[@]}" 2>/dev/null || true; fi
  rm -rf "${tmp}"
}
abort() {
  if [[ ${#loops[@]} -gt 0 ]]; then kill "${loops[@]}" 2>/dev/null || true; fi
  exit 130
}
trap cleanup EXIT
trap abort INT TERM

echo "==> request-parking load: ${DURATION}s, actors ${ACTORS[*]} against a 2-worker pool"

# Idempotent setup: "already exists" is fine.
"${HIVECTL}" create hive "${HIVE}" >/dev/null 2>&1 || true
for a in "${ACTORS[@]}"; do
  "${HIVECTL}" create actor "${a}" -H "${HIVE}" --template "${TEMPLATE}" >/dev/null 2>&1 || true
done

for i in "${!ACTORS[@]}"; do
  a="${ACTORS[${i}]}"
  "${HIVECTL}" proxy actor "${a}" "$((BASE_PORT + i + 1)):80" -H "${HIVE}" >"${tmp}/${a}.proxy.log" 2>&1 &
  proxies+=($!)
done
sleep 3

hammer() {
  local actor="$1" port="$2"
  local until=$(($(date +%s) + DURATION))
  while [[ $(date +%s) -lt ${until} ]]; do
    # Status and total time: the time shows how long a parked request waited.
    curl -s -o /dev/null -w '%{http_code} %{time_total}\n' "http://127.0.0.1:${port}" >>"${tmp}/${actor}.log" 2>/dev/null || true
    "${HIVECTL}" suspend actor "${actor}" -H "${HIVE}" >/dev/null 2>&1 || true
  done
}

for i in "${!ACTORS[@]}"; do
  hammer "${ACTORS[${i}]}" "$((BASE_PORT + i + 1))" &
  loops+=($!)
done
wait "${loops[@]}"

cat "${tmp}"/*.log >"${tmp}/all" 2>/dev/null || true
total="$(wc -l <"${tmp}/all" | tr -d ' ')"
if [[ "${total}" -eq 0 ]]; then
  echo "no responses recorded; see ${tmp}/*.proxy.log" >&2
  exit 1
fi
ok="$(awk '$1==200' "${tmp}/all" | wc -l | tr -d ' ')"
busy="$(awk '$1==503' "${tmp}/all" | wc -l | tr -d ' ')"
other=$((total - ok - busy))
slowest="$(awk '$1==200{print $2}' "${tmp}/all" | sort -rn | head -1)"
avg="$(awk '$1==200{s+=$2;n++} END{if(n)printf "%.3f", s/n; else print "n/a"}' "${tmp}/all")"

echo
echo "==> results"
printf '    requests        : %s\n' "${total}"
printf '    200 OK          : %s\n' "${ok}"
printf '    503 unavailable : %s\n' "${busy}"
printf '    other           : %s\n' "${other}"
printf '    200 latency     : avg %ss, slowest %ss (parked requests are the slow ones)\n' "${avg}" "${slowest:-n/a}"
if [[ "${busy}" -eq 0 ]]; then
  echo "    => no request was shed: parking absorbed the contention"
else
  echo "    => ${busy} requests were shed with 503 (parking off, lot full or budget exceeded)"
fi
