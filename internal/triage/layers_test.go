package triage

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
)

func plan() *flink.Job {
	j := &flink.Job{Vertices: []flink.Vertex{{ID: "src", Name: "Source"}, {ID: "map", Name: "Map"}, {ID: "sink", Name: "Sink"}}}
	j.Plan.Nodes = []flink.PlanNode{{ID: "src"}, {ID: "map"}, {ID: "sink"}}
	j.Plan.Nodes[1].Inputs = append(j.Plan.Nodes[1].Inputs, struct {
		ID string `json:"id"`
	}{ID: "src"})
	j.Plan.Nodes[2].Inputs = append(j.Plan.Nodes[2].Inputs, struct {
		ID string `json:"id"`
	}{ID: "map"})
	return j
}

func vm(id, name string, source, sink bool, busy, bp, in, out float64) VertexMetrics {
	return VertexMetrics{ID: id, Name: name, Source: source, Sink: sink, Metrics: map[string]flink.Metric{
		MetricBusy: {Max: busy}, MetricBackPressed: {Max: bp}, MetricIn: {Sum: in}, MetricOut: {Sum: out},
	}}
}

func TestPlan(t *testing.T) {
	j := plan()
	if s := j.Sources(); len(s) != 1 || s[0] != "src" {
		t.Errorf("sources %v", s)
	}
	if s := j.Sinks(); len(s) != 1 || s[0] != "sink" {
		t.Errorf("sinks %v", s)
	}
	if d := j.Downstream("src"); len(d) != 1 || d[0] != "map" || j.Vertex("map").Name != "Map" || j.Vertex("x") != nil {
		t.Errorf("downstream %v", d)
	}
}

func TestPerformanceSignals(t *testing.T) {
	collect := func(vms []VertexMetrics) string {
		var out []string
		performanceSignals(plan(), vms, func(kind string, _ Layer, text string) { out = append(out, kind+" "+text) })
		return strings.Join(out, "|")
	}
	healthy := []VertexMetrics{vm("src", "Source", true, false, 30, 0, 0, 200), vm("map", "Map", false, false, 40, 0, 200, 200), vm("sink", "Sink", false, true, 10, 0, 200, 0)}
	if got := collect(healthy); got != "" {
		t.Errorf("healthy job: %q", got)
	}
	slow := []VertexMetrics{vm("src", "Source", true, false, 100, 870, 0, 50), vm("map", "Map", false, false, 990, 0, 50, 50), vm("sink", "Sink", false, true, 10, 0, 50, 0)}
	if got := collect(slow); got != "performance.backpressure Source: backpressured 870 ms/s|performance.bottleneck Map: busy 990 ms/s" {
		t.Errorf("backpressure: %q", got)
	}
	idle := []VertexMetrics{vm("src", "Source", true, false, 30, 0, 0, 200), vm("map", "Map", false, false, 40, 0, 200, 0), vm("sink", "Sink", false, true, 0, 0, 0, 0)}
	if got := collect(idle); got != "performance.sink-idle Sink receives nothing while the sources emit 200 records/s" {
		t.Errorf("sink idle: %q", got)
	}
	// A quiet source makes a quiet sink normal.
	idle[0] = vm("src", "Source", true, false, 0, 0, 0, 0)
	if got := collect(idle); got != "" {
		t.Errorf("quiet source: %q", got)
	}
}

func TestHA(t *testing.T) {
	if HAEnabled(&model.Job{Config: map[string]string{"high-availability.type": "NONE"}}) ||
		!HAEnabled(&model.Job{Config: map[string]string{"high-availability.type": "kubernetes"}}) ||
		!HAEnabled(&model.Job{Config: map[string]string{"high-availability": "zookeeper"}}) {
		t.Error("HAEnabled")
	}
	f := kubetest.New(t)
	for name, keys := range map[string][]string{
		"ha-cluster-config-map":    {"executionPlan-old", "jobGraph-cur", "org.apache.flink.k8s.leader.dispatcher"},
		"ha-cur-config-map":        {"checkpointID-0000000000000000012", "counter"},
		"ha-stale-config-map":      {"counter"},
		"flink-config-ha":          {"config.yaml"},
		"other-cluster-config-map": {"executionPlan-x"},
	} {
		data := map[string]any{}
		for _, k := range keys {
			data[k] = ""
		}
		f.Put("configmaps", map[string]any{"metadata": map[string]any{"name": name, "namespace": "ns", "labels": map[string]any{"app": "ha"}}, "data": data})
	}
	c, err := kube.NewClient(f.Start())
	if err != nil {
		t.Fatal(err)
	}
	st, err := collectHA(context.Background(), c, "ns", "ha")
	if err != nil || strings.Join(st.Plans, ",") != "cur,old" || strings.Join(st.Checkpoints, ",") != "cur,stale" || len(st.ConfigMaps) != 3 {
		t.Fatalf("%+v %v", st, err)
	}
	var got []string
	app := &model.Job{Kind: model.KindDeployment, JobID: "cur"}
	haSignals(st, app, func(_ string, _ Layer, text string) { got = append(got, text) })
	if strings.Join(got, "|") != "job plan of old, not the current job cur|checkpoint pointers of stale, not the current job cur" {
		t.Errorf("%q", got)
	}
	// A session cluster runs several jobs; nothing is left over there.
	for _, j := range []*model.Job{{Kind: model.KindDeployment, SessionCluster: true}, {Kind: model.KindSessionJob, JobID: "cur"}, {Kind: model.KindDeployment}} {
		haSignals(st, j, func(_ string, _ Layer, text string) { t.Errorf("%+v: %s", j, text) })
	}
	f.Forbidden["configmaps/ns"] = true
	if _, err := collectHA(context.Background(), c, "ns", "ha"); err == nil {
		t.Error("forbidden: want error")
	}
}

func TestBottleneckSearchesEveryBranch(t *testing.T) {
	j := &flink.Job{Vertices: []flink.Vertex{{ID: "src"}, {ID: "a", Name: "A"}, {ID: "b", Name: "B"}}}
	in := func(id string) []struct {
		ID string `json:"id"`
	} {
		return []struct {
			ID string `json:"id"`
		}{{ID: id}}
	}
	j.Plan.Nodes = []flink.PlanNode{{ID: "src"}, {ID: "a", Inputs: in("src")}, {ID: "b", Inputs: in("src")}}
	vms := []VertexMetrics{vm("src", "Source", true, false, 100, 870, 0, 50), vm("a", "A", false, true, 10, 0, 25, 0), vm("b", "B", false, true, 990, 0, 25, 0)}
	var got []string
	performanceSignals(j, vms, func(kind string, _ Layer, text string) { got = append(got, kind+" "+text) })
	if strings.Join(got, "|") != "performance.backpressure Source: backpressured 870 ms/s|performance.bottleneck B: busy 990 ms/s" {
		t.Errorf("%q", got)
	}
}

// A sink whose metrics could not be read is not a sink that receives nothing.
func TestMissingMetricsAreNotZero(t *testing.T) {
	vms := []VertexMetrics{vm("src", "Source", true, false, 30, 0, 0, 200), {ID: "sink", Name: "Sink", Sink: true}}
	performanceSignals(plan(), vms, func(kind string, _ Layer, text string) { t.Errorf("%s %s", kind, text) })
}

func TestRuntimeSignals(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	failures := make([]flink.Exception, 4)
	for i := range failures {
		failures[i] = flink.Exception{Name: "x.Y", Stacktrace: "java.net.UnknownHostException: nowhere", Timestamp: flink.Millis(now.Add(-time.Duration(i) * time.Minute).UnixMilli())}
	}
	kinds := func(state string, j *model.Job) []string {
		ev := &Evidence{Job: j, Now: now, REST: &REST{Job: &flink.Job{State: state}, Exceptions: failures}}
		signals := Signals(ev)
		out := make([]string, 0, len(signals))
		for _, s := range signals {
			out = append(out, s.Kind)
		}
		return out
	}
	app := &model.Job{Kind: model.KindDeployment, JobID: "cur"}
	if got := kinds("RESTARTING", app); !slices.Contains(got, SigRestarts) {
		t.Errorf("restarting: %v", got)
	}
	if got := kinds("FAILED", app); slices.Contains(got, SigRestarts) {
		t.Errorf("failed for good: %v", got)
	}
	// Owning restartNonce is not a restart; a SpecChanged event that changed it is.
	app.SpecWriters = []model.SpecWriter{{Manager: "argocd-controller", Fields: []string{"job", "restartNonce"}}}
	ev := &Evidence{Job: app, Now: now}
	findings, err := Evaluate(DefaultRules(), Signals(ev), app)
	if err != nil || len(findings) != 0 {
		t.Errorf("owner only: %+v %v", findings, err)
	}
	ev.Events = []kube.Event{{Reason: "SpecChanged", Type: "Normal", Message: "UPGRADE change(s) detected (Diff: FlinkDeploymentSpec[restartNonce : 1 -> 2]), starting reconciliation.",
		LastTimestamp: metav1.NewTime(now.Add(-3 * time.Minute))}}
	findings, _ = Evaluate(DefaultRules(), Signals(ev), app)
	if len(findings) != 1 || findings[0].Title != "Restarted through spec.restartNonce 3m ago; argocd-controller writes the field" {
		t.Errorf("restarted: %+v", findings)
	}
}
