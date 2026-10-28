# Sandbox demo

A stateful command sandbox: an Alpine container running inside an Actor that
executes the commands you type at a local `sandbox>` prompt. Between sessions
the Actor is suspended, and its filesystem and memory come back on resume, so
files you create in one session are there in the next.

> **No authorization at all.** The workload runs any command it receives. Keep
> it away from production clusters and untrusted networks.

## Pieces

- **Workload** (`crates/hivekeep-demo-sandbox`, binary `sandbox`): serves
  `POST /process` on `$PORT` (default 80). The body is
  `{"command": [...], "envvars": {...}, "cwd": "...", "timeout": "30s"}`; the
  reply is `{"stdout", "stderr", "exitCode", "error"}`. A command that cannot
  start, is killed by its timeout or dies on a signal reports exit code -1.
- **Client** (`sandbox-client` in the same crate): resumes the Actor through
  hivekeep-api, forwards each prompt line as `sh -c <line>` through the local
  gate proxy, and suspends the Actor when you leave.

## Run it

```sh
cargo run --release -p hivekeep-setup -- deploy demo sandbox
hivectl create actor my-sandbox -H hivekeep-demo-sandbox --template sandbox-template
hivectl proxy actor my-sandbox -H hivekeep-demo-sandbox      # keep running: localhost:8000
```

In another terminal, with hivekeep-api reachable (for example
`kubectl -n hivekeep-system port-forward svc/hivekeep-api 8443:443` and an endpoint the
client can dial in plaintext):

```sh
export HK_API_ENDPOINT=http://localhost:8443      # plus HK_API_TOKEN if required
cargo run -p hivekeep-demo-sandbox --bin sandbox-client -- \
  --hive hivekeep-demo-sandbox --name my-sandbox --gate localhost:8000
```

```text
sandbox> echo hello > note.txt
sandbox> ls -la
sandbox> exit
```

`exit`, end of input or Ctrl-C suspends the Actor. Start the client again and
`cat note.txt` still works.

`manual-test-multi.yaml` and `manual-test-multi-template.yaml` are a variant
with two external volumes (hostpath and NFS CSI) for manual checks.

## Remove it

```sh
hivectl delete actor my-sandbox -H hivekeep-demo-sandbox
cargo run --release -p hivekeep-setup -- delete demo sandbox
```
