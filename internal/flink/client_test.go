package flink_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/flink/flinktest"
)

const fixtures = "../../testdata/fixtures/"

// TestDecodeFixtures guards the fields the tool reads on every captured Flink version: REST has no schema, so
// this is where a renamed field shows up.
func TestDecodeFixtures(t *testing.T) {
	dirs, _ := filepath.Glob(fixtures + "operator-*")
	scenarios, _ := filepath.Glob(fixtures + "scenarios-*/tm-lost")
	dirs = append(dirs, scenarios...)
	if len(dirs) < 5 {
		t.Fatalf("found %d fixture directories", len(dirs))
	}
	ctx := context.Background()
	for _, dir := range dirs {
		srv, jid := flinktest.Server(t, dir)
		c := flink.New(srv.URL)
		name := filepath.Base(filepath.Dir(dir)) + "/" + filepath.Base(dir)
		cfg, err := c.Config(ctx)
		if err != nil || cfg.FlinkVersion == "" {
			t.Errorf("%s config: %+v %v", name, cfg, err)
		}
		jobs, err := c.Jobs(ctx)
		if err != nil || len(jobs) == 0 || jobs[0].JID != jid || jobs[0].State == "" {
			t.Errorf("%s jobs: %+v %v", name, jobs, err)
		}
		job, err := c.Job(ctx, jid)
		if err != nil || job.State != "RUNNING" || len(job.Vertices) == 0 || job.Vertices[0].Name == "" || job.Timestamps["RUNNING"].Time().IsZero() {
			t.Errorf("%s job: %+v %v", name, job, err)
		}
		cps, err := c.Checkpoints(ctx, jid)
		// One capture caught the job before its first checkpoint completed, with only failures counted.
		if err != nil || cps.Counts.Total == 0 ||
			cps.Counts.Completed > 0 && (cps.Latest.Completed == nil || cps.Latest.Completed.LatestAck.Time().IsZero() ||
				len(cps.History) == 0 || cps.History[0].Status == "") {
			t.Errorf("%s checkpoints: %+v %v", name, cps, err)
		}
		cc, err := c.CheckpointConfig(ctx, jid)
		if err != nil || cc.Interval == 0 || cc.Timeout == 0 || cc.Mode == "" {
			t.Errorf("%s checkpoint config: %+v %v", name, cc, err)
		}
		if _, err := c.Exceptions(ctx, jid, 20); err != nil {
			t.Errorf("%s exceptions: %v", name, err)
		}
	}
}

// TestTaskManagerLost reads the failure the tm-lost scenario induced, on every captured Flink version.
func TestTaskManagerLost(t *testing.T) {
	dirs, _ := filepath.Glob(fixtures + "scenarios-*/tm-lost")
	if len(dirs) == 0 {
		t.Fatal("no tm-lost fixtures")
	}
	for _, dir := range dirs {
		srv, jid := flinktest.Server(t, dir)
		c := flink.New(srv.URL)
		ex, err := c.Exceptions(context.Background(), jid, 20)
		if err != nil || len(ex.History.Entries) == 0 {
			t.Fatalf("%s: %+v %v", dir, ex, err)
		}
		e := ex.History.Entries[0]
		if e.Timestamp.Time().IsZero() || !strings.Contains(e.Message(), "Exception") || e.Where() == "" || e.TaskName == "" {
			t.Errorf("%s: entry %+v", dir, e)
		}
		cps, err := c.Checkpoints(context.Background(), jid)
		if err != nil || cps.Latest.Restored == nil || cps.Latest.Restored.RestoredAt.Time().IsZero() {
			t.Errorf("%s: restored %+v %v", dir, cps.Latest.Restored, err)
		}
	}
}

func TestHTTPError(t *testing.T) {
	srv, _ := flinktest.Server(t, fixtures+"operator-1.16.1-flink-2.2")
	_, err := flink.New(srv.URL).Job(context.Background(), "0000")
	if !flink.IsNotFound(err) || !strings.Contains(err.Error(), "404 Not found: /jobs/0000") {
		t.Errorf("%v", err)
	}
}

func TestExceptionText(t *testing.T) {
	e := flink.Exception{Name: "x.Y", Stacktrace: "org.a.Outer: wrapped\n\tat a.b\nCaused by: java.io.IOException: disk\n\tat c.d\nCaused by: java.net.ConnectException: refused\n\tat e.f"}
	if e.Message() != "org.a.Outer: wrapped" || e.Cause() != "java.net.ConnectException: refused" {
		t.Errorf("%q %q", e.Message(), e.Cause())
	}
	e = flink.Exception{Name: "x.Y", Location: "10.0.0.1:1"}
	if e.Message() != "x.Y" || e.Cause() != "" || e.Where() != "10.0.0.1:1" {
		t.Errorf("%q %q %q", e.Message(), e.Cause(), e.Where())
	}
}

func TestMetricMin(t *testing.T) {
	var ms []flink.Metric
	if err := json.Unmarshal([]byte(`[{"id":"a","min":0,"max":5},{"id":"b","max":5}]`), &ms); err != nil {
		t.Fatal(err)
	}
	if !ms[0].HasMin || ms[0].Min != 0 || ms[0].Max != 5 || ms[1].HasMin || ms[1].ID != "b" {
		t.Errorf("%+v", ms)
	}
}

func TestMetricMinRoundTrip(t *testing.T) {
	for _, m := range []flink.Metric{{ID: "a", Max: 5}, {ID: "b", Max: 5, Min: 0, HasMin: true}} {
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var back flink.Metric
		if err := json.Unmarshal(b, &back); err != nil || back != m {
			t.Errorf("%+v became %s and %+v", m, b, back)
		}
	}
}
