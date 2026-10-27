# Request parking demo

When every worker of a pool is busy, the gate router does not have to fail a
request for a suspended Actor with `503`: it can **park** it and keep retrying
the resume for a short budget, until a worker frees up. This demo makes that
visible with a deliberately oversubscribed setup: a 2-worker pool and more
Actors than that, all running the counter binary (whose reply names the
worker it ran on).

Parking is on by default: `--parked-request-budget=5s` and
`--parked-request-max=1024` on the `gate-router` container.

## Run it

```sh
cargo run --release -p hivekeep-setup -- deploy demo parking
for id in p1 p2 p3 p4; do
  hivectl create actor "$id" -H hivekeep-demo-parking --template parking
done
hivectl proxy actor p1 8001:80 -H hivekeep-demo-parking &
hivectl proxy actor p2 8002:80 -H hivekeep-demo-parking &
hivectl proxy actor p3 8003:80 -H hivekeep-demo-parking &
```

### Watch one request wait

```sh
curl -s http://localhost:8001; curl -s http://localhost:8002   # both workers now busy
curl -s -w '\n-> HTTP %{http_code} in %{time_total}s\n' http://localhost:8003   # hangs: parked
```

Within five seconds, from another terminal:

```sh
hivectl suspend actor p1 -H hivekeep-demo-parking
```

The waiting `curl` completes with `200`; its time is how long it was parked.
With parking off it would have been an immediate `503`.

### Under load

[`load.sh`](load.sh) runs a request-then-suspend loop per Actor for a while
and tallies the outcomes:

```sh
demos/parking/load.sh            # 30 s, p1..p4
demos/parking/load.sh -d 60 p1 p2 p3 p4 p5 p6
```

With parking on, expect only 200s, some of them slow.

### Look inside the router

```sh
kubectl -n hivekeep-system port-forward deployment/gate-router 4040:4040
curl -s 'http://localhost:4040/statusz?format=json' | jq .parking
```

The router also exports parking metrics (active parked requests, wait time by
outcome, rejections) on its metrics port.

### Turn it off for comparison

```sh
kubectl -n hivekeep-system patch deployment gate-router --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--parked-request-max=0"}]'
kubectl -n hivekeep-system rollout status deployment/gate-router
demos/parking/load.sh            # now a burst of 503s
kubectl -n hivekeep-system rollout undo deployment/gate-router
```

Rather than disabling it you can also tune `--parked-request-budget` and
`--parked-request-max`.

## Remove it

```sh
cargo run --release -p hivekeep-setup -- delete demo parking
```
