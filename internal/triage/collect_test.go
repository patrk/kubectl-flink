package triage_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// A JobManager that restarted once and runs again: both its previous and its current log are read.
func TestCollectLogsAfterRestart(t *testing.T) {
	const dir = "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/missing-jar"
	f := kubetest.New(t)
	fd := kubetest.ReadFixture(t, filepath.Join(dir, "flinkdeployment.json"))
	f.Put("flinkdeployments", fd)
	pod := kubetest.ReadFixture(t, filepath.Join(dir, "pods.json"))
	cs := pod["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)
	cs["restartCount"] = 1
	cs["state"] = map[string]any{"running": map[string]any{"startedAt": "2026-10-24T10:00:00Z"}}
	f.Put("pods", pod)
	name := pod["metadata"].(map[string]any)["name"].(string)
	f.Logs = map[string]string{
		"default/" + name + ":previous": "ERROR old crash\n",
		"default/" + name:               "INFO fine\nERROR new problem\n",
	}
	c, err := kube.NewClient(f.Start())
	if err != nil {
		t.Fatal(err)
	}
	job := jobOf(t, fd)
	ev := triage.Collect(context.Background(), triage.Source{Kube: c}, &job, time.Now(), model.DefaultThresholds())
	got := make([]string, 0, len(ev.Logs))
	for _, l := range ev.Logs {
		got = append(got, l.Text)
	}
	if strings.Join(got, "|") != "ERROR old crash|ERROR new problem" {
		t.Errorf("log lines %q", got)
	}
}

// With a REST URL from the user, REST is asked even when the resource says the JobManager is down.
func TestCollectForcedREST(t *testing.T) {
	const dir = "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/bad-image"
	f := kubetest.New(t)
	fd := kubetest.ReadFixture(t, filepath.Join(dir, "flinkdeployment.json"))
	f.Put("flinkdeployments", fd)
	c, err := kube.NewClient(f.Start())
	if err != nil {
		t.Fatal(err)
	}
	job := jobOf(t, fd)
	for force, want := range map[bool]string{false: "not asked: the JobManager is ERROR", true: "connection refused"} {
		src := triage.Source{Kube: c, REST: flink.Direct("http://127.0.0.1:1"), ForceREST: force}
		ev := triage.Collect(context.Background(), src, &job, time.Now(), model.DefaultThresholds())
		found := false
		for _, g := range ev.Gaps {
			found = found || g.What == "JobManager REST API" && strings.Contains(g.Why, want)
		}
		if !found {
			t.Errorf("force %v: gaps %+v, want %q", force, ev.Gaps, want)
		}
	}
}
