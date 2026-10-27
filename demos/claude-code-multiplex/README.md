# Claude Code multiplex demo

Three Claude Code agents, `luna`, `mars` and `orion`, run as Hivekeep Actors
on a WorkerPool that has only **two** workers. Each agent works in a loop:
send a short prompt, print the reply, then idle for 45 seconds. While an
agent idles, Hivekeep can suspend it (snapshot its memory, release its
worker) and resume a different agent on that worker. At any moment at most
two agents are running and the third waits, suspended.

The ratio is deliberate. Fewer workers than agents forces the suspend/resume
path to run all the time, so you can see it happen. The same arrangement
holds at larger sizes, for example ten agents on three workers.

The demo talks to hivekeep-api to manage Actors. Its agents receive no
inbound HTTP, so nothing goes through the gate router.

## Layout

| Path | What it is |
|---|---|
| `crates/hivekeep-setup/manifests/demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl` | Namespace and the two-replica WorkerPool |
| `crates/hivekeep-setup/manifests/demos/claude-code-multiplex/agent-*-template.yaml.tmpl` | One ActorTemplate per agent, created through hivekeep-api |
| `demos/claude-code-multiplex/workload/` | Agent image: Node.js, the Claude Code CLI, and the `run.sh` loop |
| `demos/claude-code-multiplex/ui/index.html` | The dashboard page |
| `crates/hivekeep-demo-multiplex-ui` | `multiplex-ui`, the server behind the dashboard |

## Before you start

- A cluster with Hivekeep installed, and `kubectl` pointed at it.
- An Anthropic API key. The agents call Claude.
- The snapshot bucket you configured at install time (`BUCKET_NAME`).
- `HK_BUILD_REPO` set to a registry you can push to. The deploy step builds
  `workload/` with `docker buildx` and pushes it as
  `claude-multiplex-demo-workload`.
- A way to reach hivekeep-api. From a workstation, forward the Service in
  its own terminal and leave it running:

  ```bash
  kubectl port-forward svc/hivekeep-api 8443:443 -n hivekeep-system
  ```

  If the dashboard runs inside the cluster, its namespace has to be allowed
  by the hivekeep-system NetworkPolicies to reach the API.

## Deploy

From the repository root:

```bash
ANTHROPIC_API_KEY=sk-ant-... \
BUCKET_NAME=my-snapshot-bucket \
HK_API_ENDPOINT=http://localhost:8443 \
hivekeep-setup deploy demo claude-code-multiplex
```

This step:

1. Creates the `claude-multiplex-demo` namespace and its `claude-workerpool`
   WorkerPool.
2. Builds and pushes the agent image, then pins it by digest.
3. Creates the `claude-multiplex-demo` hive and the `agent-luna`,
   `agent-mars` and `agent-orion` templates in it. The pinned image,
   `ANTHROPIC_API_KEY` and `BUCKET_NAME` are filled in when the templates
   are applied.
4. Waits until each template has its golden snapshot.

You can check the templates with:

```bash
hivectl get actor-template -a claude-multiplex-demo
```

The installer reads one setting for this demo, on top of the usual install
settings:

| Key | Environment variable | Flag | Default |
|---|---|---|---|
| `demo.anthropicAPIKey` | `ANTHROPIC_API_KEY` | none | none (secret) |

Because the key is a secret, it has no flag, and the run report prints
`<set>` in place of the value.

## Run the dashboard

```bash
PORT=8090 HK_API_ENDPOINT=http://localhost:8443 \
  cargo run -p hivekeep-demo-multiplex-ui --bin multiplex-ui
```

Open <http://localhost:8090>. Pick a `PORT` that doesn't clash with the
port-forward.

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | Port the dashboard listens on |
| `HK_API_ENDPOINT` | none, required | hivekeep-api over plaintext gRPC, e.g. `http://localhost:8443` |
| `HK_API_TOKEN` | unset | Bearer token sent with each API call |
| `DEMO_NAMESPACE` | `claude-multiplex-demo` | The demo hive, and the namespace worker pod logs are read from |

The dashboard does not open a port-forward or mint a token for itself. You
have to provide both the endpoint and any token it needs.

It reads pod logs through the Kubernetes API, using in-cluster credentials
or your kubeconfig. If neither is available, the dashboard still starts:
the log cards stay empty, and `GET /healthz` reports `"logs": false`.

To build a container image of the dashboard from the workspace root:

```bash
docker build -f demos/claude-code-multiplex/ui/Dockerfile \
  -t REGISTRY_PLACEHOLDER/hivekeep-demo-multiplex-ui:0.1.0 .
```

## What to watch

- **Workers.** Two workers. A worker shows `Running` while it hosts an
  agent and `Idle` otherwise.
- **Actors.** Agents move from `Running` to `Suspending` to `Suspended`,
  and back through `Resuming`, as they take turns on the workers.
- **Worker logs.** One card per worker. The `[agent:<name>]` lines in a card
  switch between agents as the agents move from worker to worker.
- **Give a task.** Each click hands a random canned prompt to a random agent.
  The task shows as `queued`, then `running`, then `completed`. The dashboard
  only simulates this timing. The agents keep to their own loop.

## Tear down

```bash
hivekeep-setup delete demo claude-code-multiplex
```

This removes the demo's Actors, templates, hive and the
`claude-multiplex-demo` namespace. Stop the port-forward and the dashboard
yourself.
