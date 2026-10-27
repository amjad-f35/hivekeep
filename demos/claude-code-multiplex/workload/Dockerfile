# Agent image for the multiplex demo: Node.js plus the Claude Code CLI, driven
# by run.sh. The installer builds this directory as claude-multiplex-demo-workload.
FROM node:20-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Pin a version here for reproducible demo images.
ARG CLAUDE_CODE_VERSION=latest
RUN npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}"

COPY run.sh /usr/local/bin/agent-loop
RUN chmod 0755 /usr/local/bin/agent-loop

ENV TASK="Tell me a fact about the Moon." \
    INTERVAL_SECONDS=60

ENTRYPOINT ["/usr/local/bin/agent-loop"]
