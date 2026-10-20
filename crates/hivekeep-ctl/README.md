# hivekeep-ctl

`hivectl`, the command-line client for Hivekeep.

```sh
cargo install --path crates/hivekeep-ctl
hivectl --help
```

Commands follow `hivectl <verb> <resource>`: `get`, `create`, `update`,
`delete`, `suspend`, `resume`, `revert`, `logs`, `top`, `drain`,
`mark-available`, `mark-unavailable`, `proxy` and `admin`. Most of them call
`hivekeep-api` over gRPC (`--endpoint` / `HK_ENDPOINT`, `--token-file` /
`HK_TOKEN_FILE`, `--ca-file` / `HK_CA_FILE`). `logs`, `top` and `admin`
also use `kubectl` with your kubeconfig. `proxy` tunnels local ports to an
Actor through the gate's CONNECT-over-TLS listener. Output can be `-o
table|json|yaml`, and manifests are protobuf JSON in YAML or JSON.

`admin` manages CA pool and JWT authority pool Secrets: `make-ca-pool`,
`make-jwt-pool`, `list-jwt-keys`, `add-jwt-key`, `activate-jwt-key` and
`remove-jwt-key`.

See [docs/api-guide.md](../../docs/api-guide.md#hivectl).
