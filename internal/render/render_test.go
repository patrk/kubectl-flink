package render

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
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
		Owner:      model.Owner{Tool: model.ToolArgoCD, Name: "flink-jobs", From: "annotation argocd.argoproj.io/tracking-id", Reverts: model.RevertsSelfHeal},
	}
	ok, short, failed, sj := base, base, base, base
	ok.Name = "orders"
	short.Name, short.TMReplicas, short.Pods = "payments", 2, pods
	short.Owner.Edits = []model.SpecWriter{{Manager: "kubectl-patch", Fields: []string{"job"}, Time: ago(time.Hour)}}
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

// In the wide table the snapshot age changes with time too; only a newer snapshot reprints the row.
func TestTableWatchWide(t *testing.T) {
	rows, scopes := fixture()
	snapped := func(at time.Time) []fleet.Row {
		out := slices.Clone(rows)
		j := *out[0].Job
		j.LastSnapshot = at
		out[0].Job = &j
		return out
	}
	var b bytes.Buffer
	tbl := NewTable(&b, TableOptions{Wide: true})
	if err := tbl.Print(snapped(now.Add(-time.Minute)), scopes, now); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	if err := tbl.Update(snapped(now.Add(-time.Minute)), scopes, now.Add(time.Hour)); err != nil || b.Len() != 0 {
		t.Fatalf("age-only change printed %q %v", b.String(), err)
	}
	if err := tbl.Update(snapped(now), scopes, now); err != nil || !strings.Contains(b.String(), rows[0].Job.Name) {
		t.Errorf("newer snapshot printed %q %v", b.String(), err)
	}
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
	if got := s.Line(); got != "4 jobs: 1 FAILED, 1 DEGRADED, 2 OK · 3 pods · requests 1.5 CPU, 4Gi memory" {
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

// What a terminal shows of the coloured table lines up: after removing the colour codes, every row's lifecycle
// starts where the header's does, also for an error row and a deleted one.
func TestColorAlignment(t *testing.T) {
	rows, scopes := fixture()
	var b bytes.Buffer
	tbl := NewTable(&b, TableOptions{Color: true, ShowContext: true, ShowNamespace: true})
	if err := tbl.Print(rows, scopes, now); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Update(rows[1:], scopes, now); err != nil { // the first row is gone
		t.Fatal(err)
	}
	ansi := regexp.MustCompile("\x1b\\[[0-9;]*m")
	lines := strings.Split(strings.TrimSpace(ansi.ReplaceAllString(b.String(), "")), "\n")
	col := strings.Index(lines[0], "LIFECYCLE")
	for _, l := range lines[1:] {
		if col >= len(l) || l[col-1] != ' ' || l[col] == ' ' {
			t.Errorf("misaligned:\n%s\n%s", lines[0], l)
		}
	}
	if !strings.Contains(b.String(), "deleted") {
		t.Fatal("no deleted row")
	}
}

// A failure that repeats with every restart is one line with a count, and the rest are counted as lines.
func TestCollapseLogLines(t *testing.T) {
	l := func(s string) triage.LogLine { return triage.LogLine{Pod: "jm", Text: s} }
	got := collapse([]triage.LogLine{l("a"), l("b"), l("a"), l("a"), {Pod: "jm", Previous: true, Text: "a"}})
	if len(got) != 3 || got[0].Text != "a" || got[0].n != 3 || got[1].n != 1 || !got[2].Previous {
		t.Errorf("%+v", got)
	}
	if cores("102500m") != "102.5" || cores("100600m") != "100.6" || cores("2") != "2" || cores("1234m") != "1.23" || cores("x") != "x" {
		t.Errorf("cores %s %s %s %s", cores("102500m"), cores("100600m"), cores("1234m"), cores("x"))
	}
}

func TestWhyOwner(t *testing.T) {
	why := func(j *model.Job) string {
		var b bytes.Buffer
		if err := Why(&b, &triage.Evidence{Job: j, Now: now}, &triage.Verdict{}); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	j := &model.Job{Name: "fraud", SpecOwners: []string{"argocd-controller", "kubectl-edit"}, SpecChangedAt: ago(time.Hour),
		Owner: model.Owner{Tool: model.ToolArgoCD, Name: "payments-jobs", From: "annotation argocd.argoproj.io/tracking-id", Reverts: model.RevertsSelfHeal,
			Edits: []model.SpecWriter{{Manager: "kubectl-edit", Fields: []string{"job"}, Time: ago(time.Hour)}}}}
	got := why(j)
	for _, want := range []string{
		"spec written by argocd-controller, kubectl-edit, last 60m ago",
		"owned by Argo CD payments-jobs (from annotation argocd.argoproj.io/tracking-id); it reverts a manual edit of the spec when the Application has self-heal on",
		"spec job changed outside Argo CD by kubectl-edit, last 60m ago",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	// Without a tool, the writers are named as before.
	j.Owner = model.Owner{}
	if got := why(j); !strings.Contains(got, "spec written by argocd-controller, kubectl-edit, last 60m ago") || strings.Contains(got, "owned by") {
		t.Errorf("last writer:\n%s", got)
	}
}
