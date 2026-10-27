# Multi-template demo

Two ActorTemplates, two different binaries, two different hives, **one**
WorkerPool. A template picks its pool with `workerSelector`, a label selector
over pool labels that is evaluated cluster-wide, so neither hives nor
namespaces have to match the pool's.

| Template | Hive | Binary | What it proves |
|----------|------|--------|----------------|
| `counter` | `hivekeep-demo-multi-template-counter` | counter demo | memory survives suspend/resume |
| `fspersist` | `hivekeep-demo-multi-template-fspersist` | `crates/hivekeep-demo-fspersist` | the root filesystem survives too |

`fspersist` prepends `pod=<address> | count=<n> | time=<RFC 3339>` to
`/pod-history.log` on every request (keeping the newest 20 lines) and returns
the history. The count is read back from the file, not memory, and the
recorded addresses show each move to another worker. Its image is built from
[`fspersist/Dockerfile`](fspersist/Dockerfile).

## Run it

```sh
cargo run --release -p hivekeep-setup -- deploy demo multi-template
hivectl create actor c1 -H hivekeep-demo-multi-template-counter --template counter
hivectl create actor f1 -H hivekeep-demo-multi-template-fspersist --template fspersist
hivectl proxy actor c1 8000:80 -H hivekeep-demo-multi-template-counter &
hivectl proxy actor f1 8001:80 -H hivekeep-demo-multi-template-fspersist &

curl -s http://localhost:8000     # hello from: <ip> | preserved memory count: 1 | ...
curl -s http://localhost:8001     # pod: <ip> / --- history --- / pod=<ip> | count=0 | ...
hivectl get workers               # both Actors on workers of the one shared pool

hivectl suspend actor f1 -H hivekeep-demo-multi-template-fspersist
curl -s http://localhost:8001     # the history is intact and the count keeps rising
```

## Remove it

```sh
cargo run --release -p hivekeep-setup -- delete demo multi-template
```

This removes both Actors, both templates and hives, the pool and its
namespace.
