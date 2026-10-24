package triage_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/flink/flinktest"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// scenario serves a captured scenario, its resource, pods, events, JobManager logs and REST answers, and
// collects its evidence the way why does, at the time of the capture.
func scenario(t *testing.T, dir string) *triage.Evidence {
	t.Helper()
	f := kubetest.New(t)
	fd := kubetest.ReadFixture(t, filepath.Join(dir, "flinkdeployment.json"))
	f.Put("flinkdeployments", fd)
	f.PutAll("pods", filepath.Join(dir, "pods.json"))
	f.PutAll("events", filepath.Join(dir, "events.json"))
	if _, err := os.Stat(filepath.Join(dir, "configmaps.json")); err == nil {
		f.PutAll("configmaps", filepath.Join(dir, "configmaps.json"))
	}
	ns := fd["metadata"].(map[string]any)["namespace"].(string)
	logs, _ := filepath.Glob(filepath.Join(dir, "logs", "*.log"))
	f.Logs = map[string]string{}
	for _, l := range logs {
		b, err := os.ReadFile(l)
		if err != nil {
			t.Fatal(err)
		}
		pod, previous := strings.CutSuffix(strings.TrimSuffix(filepath.Base(l), ".log"), ".previous")
		key := ns + "/" + pod
		if previous {
			key += ":previous"
		}
		f.Logs[key] = string(b)
	}
	c, err := kube.NewClient(f.Start())
	if err != nil {
		t.Fatal(err)
	}
	job := jobOf(t, fd)
	// Like why, always offer REST; Collect asks only when the JobManager is ready. An address nothing listens
	// on fails loudly if it does ask without a capture.
	src := triage.Source{Kube: c, REST: flink.Direct("http://127.0.0.1:1")}
	now := captureTime(t, dir, &job)
	if _, err := os.Stat(filepath.Join(dir, "rest")); err == nil {
		srv, _ := flinktest.Server(t, dir)
		src.REST = flink.Direct(srv.URL)
	}
	return triage.Collect(context.Background(), src, &job, now, model.DefaultThresholds())
}

// jobOf converts a fixture FlinkDeployment as resolveJob would.
func jobOf(t *testing.T, fd map[string]any) model.Job {
	t.Helper()
	var obj kube.FlinkDeployment
	b, _ := json.Marshal(fd)
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatal(err)
	}
	return model.FromDeployment("kind", &obj)
}

// captureTime is when the scenario was captured: the JobManager's clock if REST was captured, else the latest
// event or reconcile, so that ages in goldens are stable.
func captureTime(t *testing.T, dir string, job *model.Job) time.Time {
	var rest struct {
		Now flink.Millis `json:"now"`
	}
	if b, err := os.ReadFile(filepath.Join(dir, "rest", "job.json")); err == nil && json.Unmarshal(b, &rest) == nil {
		return rest.Now.Time()
	}
	latest := job.ReconciledAt
	var events kube.EventList
	b, err := os.ReadFile(filepath.Join(dir, "events.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &events); err != nil {
		t.Fatal(err)
	}
	for i := range events.Items {
		if l := events.Items[i].Last(); l.After(latest) {
			latest = l
		}
	}
	return latest.Add(time.Minute)
}

func judge(t *testing.T, ev *triage.Evidence) *triage.Verdict {
	t.Helper()
	v, err := triage.Judge(ev, triage.Signals(ev), triage.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func ids(v *triage.Verdict) string {
	out := make([]string, 0, len(v.Findings))
	for _, f := range v.Findings {
		out = append(out, f.ID)
	}
	return strings.Join(out, ",")
}

// TestScenarioVerdicts is M2's done criterion on captured failures: why names the induced cause first, on every
// captured Flink version.
func TestScenarioVerdicts(t *testing.T) {
	// Health is what ls shows and runs on timers, so failures captured within the grace period are still
	// PROGRESSING; the rules already name them.
	want := map[string]struct {
		first  string
		health model.Health
	}{
		"bad-image":      {"image-pull", model.Progressing},
		"missing-jar":    {"jar-missing", model.Progressing},
		"missing-secret": {"mount-failed", model.Progressing},
		"unschedulable":  {"unschedulable", model.Progressing},
		"bad-memory":     {"memory-config", model.Progressing},
		"tm-lost":        {"taskmanager-lost", model.OK},
		"suspended":      {"", model.Suspended},
		"unreachable":    {"dependency-unreachable", model.Failed},
		"restart-loop":   {"dependency-unreachable", model.OK},
		"restart-bot":    {"external-restart", model.OK},
		"ha":             {"", model.OK},
	}
	dirs, _ := filepath.Glob("../../testdata/fixtures/scenarios-*/*")
	if len(dirs) < 2*len(want) {
		t.Fatalf("found %d scenario fixtures, want %d", len(dirs), 2*len(want))
	}
	for _, dir := range dirs {
		name := filepath.Base(dir)
		w, ok := want[name]
		if !ok {
			t.Errorf("no expectation for scenario %s", dir)
			continue
		}
		ev := scenario(t, dir)
		v := judge(t, ev)
		first := ""
		if len(v.Findings) > 0 {
			first = v.Findings[0].ID
		}
		if first != w.first {
			t.Errorf("%s: findings %s, want %s first; signals %+v", dir, ids(v), w.first, triage.Signals(ev))
		}
		if v.Health != w.health {
			t.Errorf("%s: health %s, want %s; %+v", dir, v.Health, w.health, ev.Health.Findings)
		}
	}
}
