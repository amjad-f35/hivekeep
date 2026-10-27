# Counter demo

A tiny HTTP server whose every response reports three counters kept in three
different places:

| Counter | Lives in | Survives a `MEMORY` snapshot | `ROOTFS` | `VOLUMES` |
|---------|----------|:---:|:---:|:---:|
| memory count | process memory | yes | no (cold boot) | no |
| file counter | `/home/counter/a.txt`, a durable volume | yes | yes | yes |
| rootfs counter | `/rootfs-counter.txt` on the container's root filesystem | yes | yes | no |

The template asks for `SNAPSHOT_FIDELITY_MEMORY`, so all three keep counting
across suspend and resume, even when the Actor wakes on another worker. The
first field of each reply is the worker's address, which makes moves visible.

The WorkerPool is a Kubernetes resource; the ActorTemplate is a Hivekeep
resource that lives in the `hivekeep-demo-counter` **hive** and is managed
through hivekeep-api with `hivectl`.

## Workload

`crates/hivekeep-demo-counter` (binary `counter`, at `/app/counter` in the
image built by [`Dockerfile`](Dockerfile)). Endpoints on port 80:

- any path: bump and report the counters;
- `/readyz`: 200 once start-up (writing a 1 MiB random file, whose hash is
  logged every 10 s) has finished; after a restore it is ready at once;
- `/set-sigterm-sleep?duration=<secs>`: how long to linger after SIGTERM
  (default 15 s), for exercising graceful termination.

Flags: `--file-counter-directory`, `--second-file-counter-directory` (a second
durable volume), `--rootfs-counter-file`, `--validate-existing-file-path`
(echo a file's content, e.g. from an external volume), `--extra-port` (a
second HTTP listener that names its port) and `--tcp-port` (a raw TCP echo).

## Run it

```sh
# gVisor workers
cargo run --release -p hivekeep-setup -- deploy demo counter
# or, with an external volume mounted at /external-data
cargo run --release -p hivekeep-setup -- deploy demo counter --with-external-volume --preview=ExternalVolumes

# micro-VM workers (installs the micro-VM SandboxConfig first)
scripts/install-microvm-deps.sh --install
cargo run --release -p hivekeep-setup -- deploy demo counter-microvm
# or the whole bring-up on Kind in one go:
scripts/microvm/demo-kind.sh
```

The deploy applies the pool, creates the hive and the `counter` template,
and waits for the template's golden snapshot. Then:

```sh
hivectl create actor my-counter -H hivekeep-demo-counter --template counter
hivectl proxy actor my-counter -H hivekeep-demo-counter        # forwards localhost:8000 -> :80
curl -X POST http://localhost:8000                              # resumes the Actor on demand
hivectl get actor my-counter -H hivekeep-demo-counter           # RUNNING, with its worker
hivectl suspend actor my-counter -H hivekeep-demo-counter       # add --mode=sync to wait for the upload
curl -X POST http://localhost:8000                              # every counter carries on
hivectl delete actor my-counter -H hivekeep-demo-counter
```

For micro-VM workers use hive `hivekeep-demo-counter-microvm` and template
`counter-microvm`.

### Other ports

The template starts the counter with `--extra-port=9090` and `--tcp-port=9091`.
Forward more ports by listing them:

```sh
hivectl proxy actor my-counter 8000:80 9090:9090 -H hivekeep-demo-counter
curl http://localhost:9090/        # "hello from extra port 9090 ..."
```

A request on any forwarded port resumes the Actor like one on port 80.

### Manual CSI check

`counter-microvm-csi-test.yaml` and `counter-microvm-csi-test-template.yaml`
run the counter on a micro-VM worker with its data directory on a
hostpath-CSI volume (`scripts/csi/hostpath-kind.sh` first).

## Settings

On top of the installer's own settings:

| Key | Environment | Flag | Default |
|-----|-------------|------|---------|
| `demo.counter.storageClass` | `STORAGE_CLASS` | `--storage-class` | `standard` |
| `demo.counter.withExternalVolume` | `HK_DEMO_COUNTER_WITH_EXTERNAL_VOLUME` | `--with-external-volume` | `false` |

## Remove it

```sh
cargo run --release -p hivekeep-setup -- delete demo counter          # or counter-microvm
```

This deletes the demo's Actors, the template (with its golden Actor and
snapshot), the hive and the pool.
