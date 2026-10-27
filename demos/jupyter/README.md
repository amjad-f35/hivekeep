# Jupyter demo

An unmodified upstream notebook image running as a Hivekeep Actor. Nothing in
the image knows about Hivekeep: the notebook is suspended to a snapshot when
you are done and comes back, kernel state included, the next time your
browser sends a request.

The template (`crates/hivekeep-setup/manifests/demos/jupyter/jupyter-template.yaml.tmpl`)
pins the image by digest and starts the server without a password, so keep
this to local or private clusters.

## Run it on Kind

```sh
scripts/kind/up.sh
scripts/install/hivekeep-kind.sh deploy hivekeep-system \
  --credential-provider='{"name":"k8s.io"}' \
  --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client
scripts/install/hivekeep-kind.sh deploy demo jupyter
```

(`make kind-up install-kind install-kind-demo DEMO=jupyter` does the same.)

## Use it

```sh
hivectl create actor notebook -H hivekeep-demo-jupyter --template jupyter
hivectl proxy actor notebook 8888:80 -H hivekeep-demo-jupyter
```

Open <http://localhost:8888>, create a notebook and run a cell. Then:

```sh
hivectl suspend actor notebook -H hivekeep-demo-jupyter
hivectl get actor notebook -H hivekeep-demo-jupyter      # SUSPENDED
```

Either `hivectl resume actor notebook -H hivekeep-demo-jupyter`, or just reload
the page: the request resumes the Actor and is served once it is back.

## Remove it

```sh
scripts/install/hivekeep-kind.sh delete demo jupyter
scripts/kind/down.sh
```
