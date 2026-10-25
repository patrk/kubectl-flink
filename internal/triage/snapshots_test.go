package triage

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
)

func snapshot(name, typ, state string, failures int, created, result time.Time, errMsg string) kube.FlinkStateSnapshot {
	s := kube.FlinkStateSnapshot{}
	s.Name, s.CreationTimestamp = name, metav1.NewTime(created)
	if typ == "CHECKPOINT" {
		s.Spec.Checkpoint = []byte("{}")
	}
	s.Status.State, s.Status.Failures, s.Status.Error, s.Status.Path = state, failures, errMsg, ""
	if !result.IsZero() {
		s.Status.ResultTimestamp = result.Format(time.RFC3339Nano)
	}
	return s
}

func TestSnapshotSignals(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	snaps := []kube.FlinkStateSnapshot{
		snapshot("fresh", "SAVEPOINT", "", 0, ago(30*time.Second), time.Time{}, ""),
		snapshot("stale-1", "SAVEPOINT", "", 0, ago(10*time.Minute), time.Time{}, ""),
		snapshot("stale-2", "CHECKPOINT", "", 0, ago(5*time.Minute), time.Time{}, ""),
		snapshot("done", "CHECKPOINT", "COMPLETED", 0, ago(3*time.Minute), ago(2*time.Minute), ""),
		snapshot("retrying", "SAVEPOINT", "TRIGGER_PENDING", 2, ago(4*time.Minute), ago(time.Minute),
			`{"type":"x.ReconciliationException","message":"java.lang.IllegalArgumentException: Either the savepoint path in the spec or configuration execution.checkpointing.savepoint-dir in the Flink resource has to be supplied"}`),
	}
	var got []string
	snapshotSignals(snaps, now, func(kind string, _ Layer, text string, _ ...time.Time) { got = append(got, kind+": "+text) })
	want := []string{
		"snapshot.completed: CHECKPOINT done 2m ago: ",
		"snapshot.failing: SAVEPOINT retrying TRIGGER_PENDING after 2 failures: java.lang.IllegalArgumentException: Either the savepoint path in the spec or configuration execution.checkpointing.savepoint-dir in the Flink resource has to be supplied",
		"snapshot.ignored: 2 FlinkStateSnapshots without a status, the oldest created 10m ago",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// The failing savepoint is named by its specific rule, which supersedes the general one.
	findings, err := Evaluate(DefaultRules(), signalsOf(t, got), &model.Job{Namespace: "ns", Name: "j"})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(findings))
	for _, f := range findings {
		ids = append(ids, f.ID)
	}
	if strings.Join(ids, ",") != "snapshots-ignored,savepoint-dir-missing,snapshot-failing" {
		t.Errorf("findings %v", ids)
	}
}

func signalsOf(t *testing.T, lines []string) []Signal {
	t.Helper()
	out := make([]Signal, 0, len(lines))
	for _, l := range lines {
		kind, text, _ := strings.Cut(l, ": ")
		out = append(out, Signal{Kind: kind, Text: text})
	}
	return out
}

func TestAttachSnapshots(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	mk := func(job, state string, result time.Time) kube.FlinkStateSnapshot {
		s := snapshot(job+"-x", "SAVEPOINT", state, 0, result, result, "")
		s.Namespace = "ns"
		s.Labels = map[string]string{kube.LabelSnapshotKind: "FlinkDeployment", kube.LabelSnapshotJob: job}
		return s
	}
	jobs := []model.Job{{Kind: model.KindDeployment, Namespace: "ns", Name: "a"}, {Kind: model.KindDeployment, Namespace: "ns", Name: "b"}}
	model.AttachSnapshots(jobs, []kube.FlinkStateSnapshot{
		mk("a", "COMPLETED", now.Add(-time.Hour)), mk("a", "COMPLETED", now.Add(-time.Minute)), mk("a", "FAILED", now), mk("b", "FAILED", now),
	})
	if !jobs[0].LastSnapshot.Equal(now.Add(-time.Minute)) || !jobs[1].LastSnapshot.IsZero() {
		t.Errorf("%+v / %+v", jobs[0], jobs[1])
	}
}

// Old snapshots are history: only the newest completed one of each type is a signal, and a failure counts only
// while no newer snapshot of its type has completed.
func TestSnapshotSignalsHistory(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	snaps := []kube.FlinkStateSnapshot{ // newest first
		snapshot("sp-new", "SAVEPOINT", "COMPLETED", 0, ago(time.Minute), ago(time.Minute), ""),
		snapshot("cp-abandoned", "CHECKPOINT", "ABANDONED", 0, ago(time.Hour), time.Time{}, ""),
		snapshot("sp-old", "SAVEPOINT", "COMPLETED", 0, ago(2*time.Hour), ago(2*time.Hour), ""),
		snapshot("sp-failed", "SAVEPOINT", "FAILED", 3, ago(3*time.Hour), ago(3*time.Hour), ""),
		snapshot("cp-no-result", "CHECKPOINT", "COMPLETED", 0, ago(4*time.Hour), time.Time{}, ""),
	}
	var got []string
	snapshotSignals(snaps, now, func(kind string, _ Layer, text string, _ ...time.Time) { got = append(got, kind+": "+text) })
	want := []string{
		"snapshot.completed: SAVEPOINT sp-new 60s ago: ",
		"snapshot.failing: CHECKPOINT cp-abandoned ABANDONED",
		"snapshot.completed: CHECKPOINT cp-no-result 4h ago: ",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	findings, err := Evaluate(DefaultRules(), signalsOf(t, got), &model.Job{Namespace: "ns", Name: "j"})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Title != "CHECKPOINT cp-abandoned fails (ABANDONED)" || strings.Contains(findings[0].Explain, "tried") {
		t.Errorf("findings %+v", findings)
	}
}

// JobSnapshots finds processed snapshots by label and unprocessed ones by spec, and nothing of other jobs.
func TestJobSnapshots(t *testing.T) {
	f := kubetest.New(t)
	put := func(name, job string, labelled bool) {
		labels := map[string]any{}
		if labelled {
			labels = map[string]any{kube.LabelSnapshotJob: job, kube.LabelSnapshotState: "COMPLETED"}
		}
		f.Put("flinkstatesnapshots", map[string]any{
			"apiVersion": "flink.apache.org/v1beta1", "kind": "FlinkStateSnapshot",
			"metadata": map[string]any{"name": name, "namespace": "ns", "uid": name, "labels": labels,
				"creationTimestamp": time.Date(2026, 10, 24, 12, 0, len(name), 0, time.UTC).Format(time.RFC3339)},
			"spec": map[string]any{"jobReference": map[string]any{"kind": "FlinkDeployment", "name": job}},
		})
	}
	put("done", "a", true)
	put("ignored", "a", false)
	put("other", "b", true)
	put("other-ignored", "b", false)
	c, err := kube.NewClient(f.Start())
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := JobSnapshots(t.Context(), c, &model.Job{Kind: model.KindDeployment, Namespace: "ns", Name: "a"})
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(snaps))
	for i := range snaps {
		names = append(names, snaps[i].Name)
	}
	if strings.Join(names, ",") != "ignored,done" {
		t.Errorf("snapshots %v", names)
	}
}
