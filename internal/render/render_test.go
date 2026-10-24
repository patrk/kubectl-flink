package render

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

var update = flag.Bool("update", false, "rewrite golden files")

var now = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s differs:\n--- got\n%s--- want\n%s", name, got, want)
	}
}

func ago(d time.Duration) time.Time { return now.Add(-d) }

func fixture() ([]fleet.Row, []fleet.Scope) {
	pods := &model.ClusterPods{
		JM:  model.Component{Total: 1, Ready: 1, Restarts: 2},
		TM:  model.Component{Total: 2, Ready: 1, NotReadySince: ago(20 * time.Minute)},
		CPU: resource.MustParse("1500m"), Memory: resource.MustParse("4Gi"),
	}
	base := model.Job{
		Kind: model.KindDeployment, Context: "dev", Namespace: "team-a", Lifecycle: "STABLE", JobState: "RUNNING",
		JMStatus: "READY", Generation: 2, ObservedGeneration: 2, ReconciledAt: ago(3 * time.Hour),
		JobStateSince: ago(3 * time.Hour), SpecJobState: "running", FlinkRuntime: "2.2.0", Image: "flink:2.2",
		SpecOwners: []string{"argocd-controller"},
	}
	ok, short, failed, sj := base, base, base, base
	ok.Name = "orders"
	short.Name, short.TMReplicas, short.Pods = "payments", 2, pods
	failed.Name, failed.Namespace, failed.Lifecycle, failed.JobState = "replay", "team-b", "FAILED", "FAILED"
	failed.Error = model.ParseStatusError(`{"type":"org.apache.flink.FooException","message":"java.lang.OutOfMemoryError: Java heap space in the operator while it tried to read a very long reason"}`)
	failed.Generation = 3
	sj.Kind, sj.Name, sj.DeploymentName, sj.JMStatus, sj.Image = model.KindSessionJob, "enrich", "session", "", ""
	scopes := []fleet.Scope{
		{Jobs: []model.Job{ok, short, failed, sj}},
		{Note: "", Err: errors.New("context prd: Unauthorized")},
	}
	scopes[1].Context = "prd"
	scopes[0].Context = "dev"
	rows := fleet.Rows(scopes, now, model.DefaultThresholds())
	_ = fleet.Sort(rows, "health")
	return rows, scopes
}

func TestTables(t *testing.T) {
	rows, scopes := fixture()
	for name, o := range map[string]TableOptions{
		"table":         {ShowNamespace: true},
		"wide":          {ShowNamespace: true, Wide: true},
		"multi-context": {ShowNamespace: true, ShowContext: true},
		"no-headers":    {NoHeaders: true},
		"color":         {Color: true},
	} {
		var b bytes.Buffer
		if err := NewTable(&b, o).Print(rows, scopes, now); err != nil {
			t.Fatal(err)
		}
		golden(t, "table-"+name, b.Bytes())
	}
}

func TestTableWatch(t *testing.T) {
	rows, scopes := fixture()
	var b bytes.Buffer
	tbl := NewTable(&b, TableOptions{ShowNamespace: true})
	if err := tbl.Print(rows, scopes, now); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	// Time passing alone changes only ages and the durations in reasons: nothing is printed.
	if err := tbl.Update(rows, scopes, now.Add(time.Hour)); err != nil || b.Len() != 0 {
		t.Fatalf("age-only change printed %q %v", b.String(), err)
	}
	// One job changes, one is gone: one row each, in the first print's column widths.
	changed := rows[:0:0]
	for _, r := range rows {
		switch r.Job.Name {
		case "orders":
			j := *r.Job
			j.JobState = "RESTARTING"
			j.JobStateSince = now
			r = fleet.Row{Job: &j, Assessment: model.Assess(&j, now, model.DefaultThresholds())}
		case "enrich":
			continue
		}
		changed = append(changed, r)
	}
	if err := tbl.Update(changed, scopes, now); err != nil {
		t.Fatal(err)
	}
	golden(t, "table-watch-update", b.Bytes())
}

func TestJSONAndYAML(t *testing.T) {
	rows, scopes := fixture()
	var j, y bytes.Buffer
	if err := JSON(&j, NewList(rows, scopes)); err != nil {
		t.Fatal(err)
	}
	if err := YAML(&y, NewList(rows, scopes)); err != nil {
		t.Fatal(err)
	}
	golden(t, "list-json", j.Bytes())
	golden(t, "list-yaml", y.Bytes())
}

func TestNamesSummaryNotes(t *testing.T) {
	rows, scopes := fixture()
	var b bytes.Buffer
	if err := Names(&b, rows); err != nil {
		t.Fatal(err)
	}
	if b.String() != "team-b/replay\nteam-a/payments\nteam-a/enrich\nteam-a/orders\n" {
		t.Errorf("names %q", b.String())
	}
	s := Summarise(rows)
	if got := s.Line(); got != "4 jobs: 1 FAILED, 1 DEGRADED, 2 OK · 3 pods · requests 1500m CPU, 4Gi memory" {
		t.Errorf("summary %q", got)
	}
	scopes[0].Note, scopes[0].PodsUnread = "cannot list Flink resources at cluster scope", []string{"team-b", ""}
	if got := strings.Join(Notes(scopes), "|"); got != "cannot list Flink resources at cluster scope|"+
		"cannot list pods in context dev, all namespaces, team-b; TaskManager checks skipped there" {
		t.Errorf("notes %q", got)
	}
}

func TestEvents(t *testing.T) {
	rows, scopes := fixture()
	var b bytes.Buffer
	ev := NewEvents(&b, false)
	if err := ev.Update(rows, scopes); err != nil {
		t.Fatal(err)
	}
	// dev has nothing to report; prd failed.
	if n := strings.Count(b.String(), `"type":"ADDED"`); n != 4 || strings.Count(b.String(), `"type":"SCOPE"`) != 1 ||
		!strings.Contains(b.String(), `"error":"context prd: Unauthorized"`) {
		t.Errorf("first update:\n%s", b.String())
	}
	b.Reset()
	if err := ev.Update(rows, scopes); err != nil || b.Len() != 0 {
		t.Errorf("unchanged rows printed %q", b.String())
	}
	scopes[0].WatchErr = errors.New("connection reset")
	if err := ev.Update(rows[1:], scopes); err != nil || !strings.Contains(b.String(), `"type":"DELETED"`) ||
		!strings.Contains(b.String(), `"name":"replay"`) || !strings.Contains(b.String(), `"watchError":"connection reset"`) {
		t.Errorf("deleted and watch error: %q", b.String())
	}
	b.Reset()
	y := NewEvents(&b, true)
	if err := y.Update(rows[:1], nil); err != nil || !strings.HasPrefix(b.String(), "---\nitem:") {
		t.Errorf("yaml: %q", b.String())
	}
}
