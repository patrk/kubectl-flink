package render

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
	"github.com/patrk/kubectl-flink/internal/triage/triagetest"
)

// diamond is a source that fans out to two branches that join in a sink; the source is back-pressured by the
// busy left branch.
func diamond(t *testing.T, state string) *triage.Evidence {
	t.Helper()
	var job flink.Job
	err := json.Unmarshal([]byte(`{"jid":"j1","state":"`+state+`","now":1000000,"timestamps":{"RUNNING":1000},
		"vertices":[{"id":"sink","name":"Sink: orders","parallelism":2,"status":"RUNNING"},
			{"id":"src","name":"Source: Kafka","parallelism":4,"status":"RUNNING"},
			{"id":"left","name":"enrich -> lookup","parallelism":4,"status":"RUNNING"},
			{"id":"right","name":"filter","parallelism":4,"status":"RUNNING"}],
		"plan":{"nodes":[{"id":"sink","inputs":[{"id":"left"},{"id":"right"}]},{"id":"src","inputs":[]},
			{"id":"left","inputs":[{"id":"src"}]},{"id":"right","inputs":[{"id":"src"}]}]}}`), &job)
	if err != nil {
		t.Fatal(err)
	}
	m := func(busy, pressed, idle, in, out float64) map[string]flink.Metric {
		return map[string]flink.Metric{
			triage.MetricBusy: {Max: busy}, triage.MetricBackPressed: {Max: pressed}, triage.MetricIdle: {Max: idle},
			triage.MetricIn: {Sum: in}, triage.MetricOut: {Sum: out},
		}
	}
	ev := &triage.Evidence{Job: &model.Job{Context: "dev", Namespace: "team-a", Name: "orders", Kind: model.KindDeployment},
		REST: &triage.REST{Job: &job}}
	if state == "RUNNING" {
		ev.Metrics = []triage.VertexMetrics{
			{ID: "src", Name: "Source: Kafka", Source: true, Metrics: m(10, 800, 190, 0, 12500)},
			{ID: "left", Name: "enrich -> lookup", Metrics: m(990, 0, 10, 6200, 6200)},
			{ID: "right", Name: "filter", Metrics: m(40, 0, 960, 6300, 900)},
			{ID: "sink", Name: "Sink: orders", Sink: true, Metrics: m(5, 0, 995, 7100, 0)},
		}
	}
	return ev
}

func graph(t *testing.T, ev *triage.Evidence, color bool) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := Graph(&b, ev, color); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestGraph(t *testing.T) {
	golden(t, "graph-diamond", graph(t, diamond(t, "RUNNING"), false))
	golden(t, "graph-diamond-color", graph(t, diamond(t, "RUNNING"), true))
	golden(t, "graph-failed", graph(t, diamond(t, "FAILED"), false))
	noREST := diamond(t, "RUNNING")
	noREST.REST = nil
	noREST.Gaps = []triage.Gap{{Layer: triage.Runtime, What: "JobManager REST API", Why: "the JobManager is not running"}}
	golden(t, "graph-no-rest", graph(t, noREST, false))
}

// The bottleneck the graph marks is the one why names.
func TestGraphBottleneckIsWhys(t *testing.T) {
	ev := diamond(t, "RUNNING")
	got := triage.Bottlenecks(ev.REST.Job, ev.Metrics)
	if len(got) != 1 || !got["left"] {
		t.Errorf("bottlenecks %v", got)
	}
	if !bytes.Contains(graph(t, ev, false), []byte("enrich -> lookup  ×4  RUNNING  ◀ bottleneck")) {
		t.Error("bottleneck not marked")
	}
}

// A captured job: the snapshots scenario's two operators with their metrics.
func TestGraphCaptured(t *testing.T) {
	ev := triagetest.Load(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/snapshots").Evidence()
	golden(t, "graph-snapshots", graph(t, ev, false))
}

// A plan node that is not a vertex of the job is left out without shifting the others' numbers, and an operator
// with no measured time is not called back-pressured.
func TestGraphEdgeCases(t *testing.T) {
	ev := diamond(t, "RUNNING")
	ev.REST.Job.Vertices = slices.DeleteFunc(ev.REST.Job.Vertices, func(v flink.Vertex) bool { return v.ID == "right" })
	ev.Metrics[0].Metrics[triage.MetricBusy] = flink.Metric{}
	ev.Metrics[0].Metrics[triage.MetricBackPressed] = flink.Metric{}
	ev.Metrics[0].Metrics[triage.MetricIdle] = flink.Metric{}
	g := NewJobGraph(ev)
	if len(g.Nodes) != 3 || g.Nodes[2].Name != "Sink: orders" || g.Nodes[2].Index != 3 {
		t.Fatalf("nodes %+v", g.Nodes)
	}
	if !slices.Equal(g.Nodes[0].Downstream, []int{2}) {
		t.Errorf("source feeds %v", g.Nodes[0].Downstream)
	}
	if g.Nodes[0].State != "" {
		t.Errorf("no time measured, state %q", g.Nodes[0].State)
	}
	_ = graph(t, ev, false) // prints without indexing past the nodes
	if got := triage.BackpressureLevel(triage.BackpressureHigh); got != "HIGH" {
		t.Errorf("level at the threshold %s", got)
	}
}
