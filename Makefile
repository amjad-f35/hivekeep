# Developer entry points for the Hivekeep workspace. Everything here is a thin
# wrapper over cargo and scripts/; run `make help` for the list.

SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

CARGO ?= cargo
CARGO_FLAGS ?= --locked
# Crate to scope build/test/lint to; empty means the whole workspace.
PKG ?=
PKG_FLAGS := $(if $(PKG),-p $(PKG),--workspace)
KIND_CLUSTER_NAME ?= kind
export KIND_CLUSTER_NAME

# The proto build scripts need protoc: PROTOC if set, else the pinned copy
# fetched by `make protoc`, else whatever protoc is on PATH.
PINNED_PROTOC := $(CURDIR)/bin/protoc/25.3/bin/protoc
ifeq ($(origin PROTOC),undefined)
ifneq ($(wildcard $(PINNED_PROTOC)),)
export PROTOC := $(PINNED_PROTOC)
endif
endif

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Build and test
.PHONY: build build-release test test-root test-python doc
build: ## Build (PKG=<crate> to scope)
	$(CARGO) build $(CARGO_FLAGS) $(PKG_FLAGS) --all-targets

build-release: ## Release build of every binary
	$(CARGO) build $(CARGO_FLAGS) $(PKG_FLAGS) --release

test: ## Unit and integration tests (PKG=<crate> to scope)
	$(CARGO) test $(CARGO_FLAGS) $(PKG_FLAGS)

test-root: ## Root-only tests (Linux, sudo)
	scripts/test/root.sh

test-python: ## Every Python test suite, in a Python 3.10+ venv (target/python-venv)
	scripts/verify/python.sh

doc: ## Build API docs
	$(CARGO) doc $(CARGO_FLAGS) $(PKG_FLAGS) --no-deps

##@ Code quality
.PHONY: fmt fmt-check lint shellcheck verify update python-test
fmt: ## Format Rust (and proto, if clang-format is installed)
	scripts/update/fmt.sh
	@if command -v clang-format >/dev/null; then scripts/update/proto-fmt.sh; fi

fmt-check: ## Fail on unformatted Rust
	scripts/verify/fmt.sh

lint: ## clippy with warnings as errors
	$(CARGO) clippy $(CARGO_FLAGS) $(PKG_FLAGS) --all-targets -- -D warnings

shellcheck: ## Lint every shell script
	scripts/verify/shellcheck.sh

python-test: test-python ## Alias of test-python

verify: ## Every check in scripts/verify
	scripts/verify/all.sh

update: ## Every generator/formatter in scripts/update
	scripts/update/all.sh

##@ Local cluster
.PHONY: kind-up kind-down install-kind install-kind-demo csi-hostpath csi-nfs microvm-deps microvm-demo-kind
kind-up: ## Create the Kind cluster and local registry
	scripts/kind/up.sh

kind-down: ## Delete the Kind cluster
	scripts/kind/down.sh

install-kind: ## Install Hivekeep on Kind (CREDENTIAL_PROVIDER='{"name":"k8s.io"}' by default)
	scripts/install/hivekeep-kind.sh deploy hivekeep-system \
	  --credential-provider='$(or $(CREDENTIAL_PROVIDER),{"name":"k8s.io"})' \
	  --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client

DEMO ?= counter
install-kind-demo: ## Deploy one demo on Kind (DEMO=counter|egress|sandbox|parking|jupyter|multi-template|...)
	scripts/install/hivekeep-kind.sh deploy demo $(DEMO)

csi-hostpath: ## Install the hostpath CSI driver on Kind
	scripts/csi/hostpath-kind.sh

csi-nfs: ## Install the NFS server and CSI driver on Kind
	scripts/csi/nfs-kind.sh

microvm-deps: ## Install the micro-VM SandboxConfig and assets
	scripts/install-microvm-deps.sh --install

microvm-demo-kind: ## Bring up the counter-microvm demo on Kind
	scripts/microvm/demo-kind.sh

##@ End to end
.PHONY: e2e e2e-kind e2e-cleanup
e2e: ## E2E suites against KUBECTL_CONTEXT
	scripts/e2e/run.sh

e2e-kind: ## E2E suites against the Kind cluster
	scripts/e2e/run-kind.sh

e2e-cleanup: ## Delete namespaces left by failed e2e runs
	scripts/e2e/cleanup.sh

##@ Images
REGISTRY ?= REGISTRY_PLACEHOLDER
TAG ?= dev
DEMO_IMAGES := counter egress fspersist sandbox
.PHONY: demo-images $(addprefix demo-image-,$(DEMO_IMAGES)) demo-image-multiplex-ui
demo-images: $(addprefix demo-image-,$(DEMO_IMAGES)) demo-image-multiplex-ui ## Build every demo image

demo-image-counter demo-image-sandbox demo-image-egress: demo-image-%:
	docker build -f demos/$*/Dockerfile -t $(REGISTRY)/hivekeep-demo-$*:$(TAG) .

demo-image-fspersist:
	docker build -f demos/multi-template/fspersist/Dockerfile -t $(REGISTRY)/hivekeep-demo-fspersist:$(TAG) .

demo-image-multiplex-ui:
	docker build -f demos/claude-code-multiplex/ui/Dockerfile -t $(REGISTRY)/hivekeep-demo-multiplex-ui:$(TAG) .

##@ Tools
.PHONY: tools protoc
tools: ## Install pinned developer tools into bin/tools
	scripts/tools/install.sh

protoc: ## Fetch the pinned protoc
	scripts/proto/protoc.sh --print-path
