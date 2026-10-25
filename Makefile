FLINK_OPERATOR_VERSION ?= 1.16.1
FLINK_VERSION ?= 2.2
KIND_CLUSTER ?= kubectl-flink
KUBE_CONTEXT ?= kind-$(KIND_CLUSTER)
# Versions only: the demo tag holds recordings.
VERSION ?= $(shell git describe --tags --match 'v*' --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/patrk/kubectl-flink/internal/version.Version=$(VERSION) \
           -X github.com/patrk/kubectl-flink/internal/version.Commit=$(shell git rev-parse --short HEAD 2>/dev/null || echo none) \
           -X github.com/patrk/kubectl-flink/internal/version.Date=$(shell date -u +%FT%TZ)

.PHONY: build test test-committed bench perf lint snapshot kind-up kind-deps kind-down e2e e2e-go fixtures scenarios crds demo demo-image demo-clean flood flood-clean bench-flood demo-gifs

build: ; CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/kubectl-flink ./cmd/kubectl-flink
test:  ; go test -race ./...
# The tests on exactly what a commit would contain, as CI sees it.
test-committed: ; hack/test-committed.sh
bench: ; go test -run '^$$' -bench . -benchmem ./...
perf:  ; go test -tags perf -run Budget -v ./internal/fleet/ ./internal/tui/
lint:  ; golangci-lint run ./...
snapshot: ; goreleaser release --snapshot --clean

kind-up:
	kind create cluster --name $(KIND_CLUSTER) --config test/e2e/kind.yaml
	$(MAKE) kind-deps

# archive.apache.org keeps every operator release; downloads.apache.org drops a patch release when the next ships.
kind-deps:
	helm repo add --force-update flink-operator-$(FLINK_OPERATOR_VERSION) \
	  https://archive.apache.org/dist/flink/flink-kubernetes-operator-$(FLINK_OPERATOR_VERSION)/
	helm --kube-context $(KUBE_CONTEXT) upgrade --install flink-kubernetes-operator \
	  flink-operator-$(FLINK_OPERATOR_VERSION)/flink-kubernetes-operator --set webhook.create=false --wait --timeout 5m

kind-down: ; kind delete cluster --name $(KIND_CLUSTER)

# One Flink job on the operator under test, waited on until it runs.
e2e: build
	FLINK_VERSION=$(FLINK_VERSION) KUBE_CONTEXT=$(KUBE_CONTEXT) hack/e2e-smoke.sh

# Against the real API server: cluster-scope probe, parallel load, WatchList informers, port-forward to the JobManager.
e2e-go: ; KUBE_CONTEXT=$(KUBE_CONTEXT) go test -tags e2e -run TestLive -v ./internal/kube/ ./internal/flink/

# Raw CR and JobManager REST JSON for tests, from the running e2e job.
fixtures:
	KUBE_CONTEXT=$(KUBE_CONTEXT) OUT=testdata/fixtures/operator-$(FLINK_OPERATOR_VERSION)-flink-$(FLINK_VERSION) \
	  hack/capture-fixtures.sh basic

# Long-lived example jobs in three namespaces and many states, to try the plugin by hand; demo-clean removes them.
demo: demo-image ; KUBE_CONTEXT=$(KUBE_CONTEXT) FLINK_VERSION=$(FLINK_VERSION) hack/demo.sh
# The shop jobs' image: hack/demo-job built on flink:2.2 and loaded into kind, never pulled.
demo-image:
	docker build -t kubectl-flink-demo:local hack/demo-job
	kind load docker-image kubectl-flink-demo:local --name $(KIND_CLUSTER)
demo-clean: ; KUBE_CONTEXT=$(KUBE_CONTEXT) FLINK_VERSION=$(FLINK_VERSION) hack/demo.sh clean

# 2,000 more FlinkDeployments with statuses copied from real jobs, which the operator ignores; FLOOD_JOBS and
# FLOOD_NAMESPACES change the size. bench-flood times kubectl flink against kubectl on them; needs hyperfine.
flood: ; KUBE_CONTEXT=$(KUBE_CONTEXT) hack/flood.sh
flood-clean: ; KUBE_CONTEXT=$(KUBE_CONTEXT) hack/flood.sh clean
bench-flood: build ; KUBE_CONTEXT=$(KUBE_CONTEXT) hack/bench-flood.sh

# The recordings, from docs/demo/*.tape against make demo, on dark and light backgrounds, into docs/demo/out; needs vhs.
demo-gifs: build
	hack/demo-gifs.sh

# Failure states (bad image, missing jar, suspended) captured from the operator under test.
scenarios:
	KUBE_CONTEXT=$(KUBE_CONTEXT) FLINK_VERSION=$(FLINK_VERSION) \
	  OUT=testdata/fixtures/scenarios-operator-$(FLINK_OPERATOR_VERSION)-flink-$(FLINK_VERSION) hack/capture-scenarios.sh

# CRDs of each supported operator version, for the test that checks the fields the tool reads exist upstream.
crds: ; hack/fetch-crds.sh
