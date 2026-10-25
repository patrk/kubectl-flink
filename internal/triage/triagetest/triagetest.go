// Package triagetest serves captured scenarios to tests: the resource, pods, events, ConfigMaps and JobManager
// logs through a fake API server, and the JobManager REST answers through a fake JobManager.
package triagetest

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

// Scenario is a captured scenario being served.
type Scenario struct {
	Dir    string
	API    *kubetest.API
	Client *kube.Client
	// RESTURL is the fake JobManager's address; empty when no REST answers were captured.
	RESTURL string
	// REST reaches the captured JobManager answers, or an address nothing listens on when none were captured.
	REST flink.Access
	Job  *model.Job
	// Now is the time of the capture, so that ages are stable.
	Now time.Time
}

// Load serves the scenario captured in dir.
func Load(t testing.TB, dir string) *Scenario {
	t.Helper()
	f := kubetest.New(t)
	fd := kubetest.ReadFixture(t, filepath.Join(dir, "flinkdeployment.json"))
	f.Put("flinkdeployments", fd)
	f.PutAll("pods", filepath.Join(dir, "pods.json"))
	f.PutAll("events", filepath.Join(dir, "events.json"))
	for file, resource := range map[string]string{"configmaps.json": "configmaps", "snapshots.json": "flinkstatesnapshots"} {
		if _, err := os.Stat(filepath.Join(dir, file)); err == nil {
			f.PutAll(resource, filepath.Join(dir, file))
		}
	}
	ns := fd["metadata"].(map[string]any)["namespace"].(string)
	logs, _ := filepath.Glob(filepath.Join(dir, "logs", "*.txt"))
	f.Logs = map[string]string{}
	for _, l := range logs {
		b, err := os.ReadFile(l) // #nosec G304 -- test fixtures
		if err != nil {
			t.Fatal(err)
		}
		pod, previous := strings.CutSuffix(strings.TrimSuffix(filepath.Base(l), ".txt"), ".previous")
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
	job := JobOf(t, fd)
	s := &Scenario{Dir: dir, API: f, Client: c, REST: flink.Direct("http://127.0.0.1:1"), Job: &job}
	s.Now = captureTime(t, dir, &job)
	if _, err := os.Stat(filepath.Join(dir, "rest")); err == nil {
		srv, _ := flinktest.Server(t, dir)
		s.REST, s.RESTURL = flink.Direct(srv.URL), srv.URL
	}
	return s
}

// Evidence collects the scenario's evidence the way why does.
func (s *Scenario) Evidence() *triage.Evidence {
	return triage.Collect(context.Background(), triage.Source{Kube: s.Client, REST: s.REST}, s.Job, s.Now, model.DefaultThresholds())
}

// JobOf converts a fixture FlinkDeployment as resolveJob would.
func JobOf(t testing.TB, fd map[string]any) model.Job {
	t.Helper()
	var obj kube.FlinkDeployment
	b, _ := json.Marshal(fd)
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatal(err)
	}
	return model.FromDeployment("kind", &obj)
}

// captureTime is when the scenario was captured: the JobManager's clock if REST was captured, else the time the
// capture recorded, else the latest event or reconcile.
func captureTime(t testing.TB, dir string, job *model.Job) time.Time {
	var rest struct {
		Now flink.Millis `json:"now"`
	}
	if b, err := os.ReadFile(filepath.Join(dir, "rest", "job.json")); err == nil && json.Unmarshal(b, &rest) == nil { // #nosec G304
		return rest.Now.Time()
	}
	if b, err := os.ReadFile(filepath.Join(dir, "captured-at.txt")); err == nil { // #nosec G304 -- test fixtures
		at, err := time.Parse(time.RFC3339, strings.TrimSpace(string(b)))
		if err != nil {
			t.Fatal(err)
		}
		return at
	}
	latest := job.ReconciledAt
	var events kube.EventList
	b, err := os.ReadFile(filepath.Join(dir, "events.json")) // #nosec G304 -- test fixtures
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
