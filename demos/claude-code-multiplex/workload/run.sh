#!/usr/bin/env bash
# Entrypoint of one multiplex-demo agent.
#
# Every INTERVAL_SECONDS the agent sends TASK to Claude Code in one-shot mode
# and prints the reply, then sleeps. The sleep is the window in which Hivekeep
# can suspend this Actor and lend its worker to another agent.
#
# Environment:
#   TASK              prompt sent on every tick
#   INTERVAL_SECONDS  idle time between ticks
#   ACTOR_NAME        label used in log lines (defaults to the hostname)
#   ANTHROPIC_API_KEY required by Claude Code
set -euo pipefail

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "[agent] ANTHROPIC_API_KEY is not set; not starting" >&2
  exit 1
fi

TASK="${TASK:-Tell me a fact about the Moon.}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-60}"
ACTOR_NAME="${ACTOR_NAME:-$(hostname)}"
tag="[agent:${ACTOR_NAME}]"

echo "${tag} up; task=\"${TASK}\" interval=${INTERVAL_SECONDS}s"

tick=0
while true; do
  tick=$((tick + 1))
  printf '\n%s tick %d at %s\n' "${tag}" "${tick}" "$(date -u +%H:%M:%SZ)"
  echo "${tag} prompt: ${TASK}"
  echo "---"
  # A failed call must not end the loop: the agent keeps its schedule so the
  # multiplexing stays observable even while the API is unhappy.
  if ! claude --print "${TASK}" 2>&1; then
    echo "${tag} claude exited with an error"
  fi
  echo "---"
  echo "${tag} tick ${tick} done; idle for ${INTERVAL_SECONDS}s"
  sleep "${INTERVAL_SECONDS}"
done
