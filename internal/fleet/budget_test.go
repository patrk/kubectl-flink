//go:build perf

package fleet_test

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
)

// Budgets from docs/plan.md, run with make perf. The fake API adds a round trip per request, so the numbers
// cover paging and parallelism, not only decoding; a real API server is checked on kind.
const apiLatency = 30 * time.Millisecond

// fleetAPI serves jobs spread over namespaces, each with a JobManager and a TaskManager pod.
func fleetAPI(t testing.TB, namespaces, jobsPerNamespace int) (f *kubetest.API, nss []string) {
	f = kubetest.New(t)
	nss = make([]string, 0, namespaces)
	for n := range namespaces {
		ns := fmt.Sprintf("team-%d", n)
		nss = append(nss, ns)
		f.Add("flinkdeployments", fdFixture, ns, "job", jobsPerNamespace, nil)
		for _, component := range []string{"jobmanager", "taskmanager"} {
			f.Add("pods", podsFixture, ns, component, jobsPerNamespace, func(i int, o map[string]any) {
				labels := o["metadata"].(map[string]any)["labels"].(map[string]any)
				labels["app"], labels["component"] = fmt.Sprintf("job-%d", i), component
			})
		}
		// The plan's budget counts snapshots too: seven completed per job, 10,500 for 1,500 jobs.
		for i := range jobsPerNamespace {
			for k := range snapshotsPerJob {
				f.Put("flinkstatesnapshots", map[string]any{
					"apiVersion": "flink.apache.org/v1beta1", "kind": "FlinkStateSnapshot",
					"metadata": map[string]any{"name": fmt.Sprintf("job-%d-savepoint-%d", i, k), "namespace": ns,
						"creationTimestamp": "2026-10-24T10:00:00Z",
						"labels": map[string]any{"job-reference.kind": "FlinkDeployment", "job-reference.name": fmt.Sprintf("job-%d", i),
							"snapshot.state": "COMPLETED", "snapshot.type": "SAVEPOINT", "snapshot.trigger-type": "PERIODIC"}},
					"spec": map[string]any{"jobReference": map[string]any{"kind": "FlinkDeployment", "name": fmt.Sprintf("job-%d", i)},
						"savepoint": map[string]any{"formatType": "CANONICAL"}},
					"status": map[string]any{"state": "COMPLETED", "path": fmt.Sprintf("s3://bucket/savepoints/job-%d/savepoint-%d", i, k),
						"resultTimestamp": fmt.Sprintf("2026-10-24T10:%02d:00Z", k), "triggerTimestamp": fmt.Sprintf("2026-10-24T10:%02d:00Z", k)},
				})
			}
		}
	}
	f.Latency = apiLatency
	return f, nss
}

// snapshotsPerJob is the number of completed FlinkStateSnapshots per job in the budget tests.
const snapshotsPerJob = 7

// wide is the costliest query: ls -o wide also reads snapshots.
var wide = kube.Query{Snapshots: true}

// ls does what ls does: load, assess, sort and render a table.
func ls(t testing.TB, src fleet.Source) []fleet.Scope {
	scopes := fleet.Load(context.Background(), []fleet.Source{src}, wide)
	if scopes[0].Err != nil {
		t.Fatal(scopes[0].Err)
	}
	rows := fleet.Rows(scopes, time.Now(), model.DefaultThresholds())
	if err := fleet.Sort(rows, "health"); err != nil {
		t.Fatal(err)
	}
	if err := render.NewTable(io.Discard, render.TableOptions{ShowNamespace: true}).Print(rows, scopes, time.Now()); err != nil {
		t.Fatal(err)
	}
	return scopes
}

func TestBudgetLs(t *testing.T) {
	for _, tc := range []struct {
		namespaces, perNamespace int
		budget                   time.Duration
	}{
		{1, 500, 2 * time.Second},
		{4, 375, 4 * time.Second},
	} {
		f, nss := fleetAPI(t, tc.namespaces, tc.perNamespace)
		src := source(t, f, kube.Scope{Context: "dev", Namespaces: nss})
		start := time.Now()
		scopes := ls(t, src)
		took := time.Since(start)
		jobs := tc.namespaces * tc.perNamespace
		if len(scopes[0].Jobs) != jobs || scopes[0].Jobs[0].LastSnapshot.IsZero() {
			t.Fatalf("%d jobs, want %d, with snapshot ages", len(scopes[0].Jobs), jobs)
		}
		t.Logf("ls, %d jobs in %d namespaces: %s (budget %s)", jobs, tc.namespaces, took, tc.budget)
		if took > tc.budget {
			t.Errorf("ls of %d jobs took %s, budget %s", jobs, took, tc.budget)
		}
	}
}

// TestBudgetMemory measures the heap held by watch caches and derived jobs for 1,500 jobs, 3,000 pods and 10,500
// completed snapshots.
func TestBudgetMemory(t *testing.T) {
	const budget = 150 << 20
	f, nss := fleetAPI(t, 4, 375)
	f.Latency = 0
	src := source(t, f, kube.Scope{Context: "dev", Namespaces: nss})

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	go fleet.Watch(ctx, []fleet.Source{src}, wide, ch)
	scopes := next(t, ch)
	rows := fleet.Rows(scopes, time.Now(), model.DefaultThresholds())

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	held := int64(after.HeapInuse) - int64(before.HeapInuse)
	t.Logf("watch caches and %d rows hold %d MB (budget %d MB)", len(rows), held>>20, budget>>20)
	if held > budget {
		t.Errorf("heap %d MB over budget %d MB", held>>20, budget>>20)
	}
	runtime.KeepAlive(rows)
	runtime.KeepAlive(scopes)
}

// TestBudgetRedraw: a burst of 1,000 updates is published at most 4 times a second.
func TestBudgetRedraw(t *testing.T) {
	f, nss := fleetAPI(t, 1, 100)
	f.Latency = 0
	src := source(t, f, kube.Scope{Context: "dev", Namespaces: nss})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	go fleet.Watch(ctx, []fleet.Source{src}, wide, ch)
	next(t, ch)

	start := time.Now()
	go func() {
		for i := range 10 {
			f.Add("flinkdeployments", fdFixture, nss[0], "job", 100, func(_ int, o map[string]any) {
				o["status"].(map[string]any)["error"] = fmt.Sprint("update ", i)
			})
		}
	}()
	publishes := 0
	for quiet := false; !quiet; {
		select {
		case <-ch:
			publishes++
		case <-time.After(3 * fleet.Coalesce):
			quiet = publishes > 0
		}
	}
	took := time.Since(start) - 3*fleet.Coalesce
	rate := float64(publishes) / max(took.Seconds(), 1)
	t.Logf("1,000 updates: %d publishes in %s", publishes, took)
	if rate > 4 {
		t.Errorf("%.1f publishes per second, budget 4", rate)
	}
}
