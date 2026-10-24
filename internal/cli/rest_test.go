package cli

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/flink/flinktest"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/triage"
)

var update = flag.Bool("update", false, "rewrite golden files")

func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update)", err)
	}
	if got != string(want) {
		t.Errorf("%s differs:\n--- got\n%s--- want\n%s", name, got, want)
	}
}

const tmLost = "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/tm-lost"

// TestRESTCommandsPerVersion renders the tm-lost scenario of every captured Flink version.
func TestRESTCommandsPerVersion(t *testing.T) {
	dirs, _ := filepath.Glob("../../testdata/fixtures/scenarios-*/tm-lost")
	if len(dirs) < 2 {
		t.Fatalf("found %d tm-lost fixtures, want 1.20 and 2.2", len(dirs))
	}
	for _, dir := range dirs {
		version := strings.TrimPrefix(filepath.Base(filepath.Dir(dir)), "scenarios-")
		exec, url := restClusterAt(t, dir)
		for _, cmd := range []string{"exceptions", "checkpoints"} {
			code, out, errOut := exec(cmd, "tm-lost", "--rest-url", url, "--limit", "3")
			if code != ExitOK {
				t.Fatalf("%s %s: %d %s", version, cmd, code, errOut)
			}
			golden(t, cmd+"-"+version, out)
		}
	}
}

// restCluster serves the tm-lost FlinkDeployment from a fake API server and its JobManager REST API from the
// captured responses, and returns a runner whose clock is the capture time.
func restCluster(t *testing.T) (exec func(args ...string) (int, string, string), restURL string) {
	return restClusterAt(t, tmLost)
}

func restClusterAt(t *testing.T, tmLost string) (exec func(args ...string) (int, string, string), restURL string) {
	f := kubetest.New(t)
	f.Put("flinkdeployments", kubetest.ReadFixture(t, filepath.Join(tmLost, "flinkdeployment.json")))
	c := newCluster(t)
	c.add("dev", "default", f)
	srv, _ := flinktest.Server(t, tmLost)

	var job struct {
		Now flink.Millis `json:"now"`
	}
	b, err := os.ReadFile(filepath.Join(tmLost, "rest", "job.json"))
	if err != nil || json.Unmarshal(b, &job) != nil {
		t.Fatalf("job.json: %v", err)
	}
	now := job.Now.Time()
	exec = func(args ...string) (int, string, string) {
		streams, _, out, errOut := genericiooptions.NewTestIOStreams()
		o := &Options{ConfigFlags: genericclioptions.NewConfigFlags(true), IOStreams: streams, Now: func() time.Time { return now }}
		cmd := newRootCommand(o)
		cmd.SetArgs(append([]string{"--kubeconfig", c.kubeconfig}, args...))
		code := run(cmd, streams)
		return code, out.String(), errOut.String()
	}
	return exec, srv.URL
}

func TestExceptionsCommand(t *testing.T) {
	exec, url := restCluster(t)
	code, out, errOut := exec("exceptions", "tm-lost", "--rest-url", url)
	if code != ExitOK || !strings.Contains(out, "TaskExecutor is shutting down") {
		t.Fatalf("%d %s\n%s", code, errOut, out)
	}

	code, out, _ = exec("exceptions", "default/tm-lost", "--rest-url", url, "--full")
	if code != ExitOK || !strings.Contains(out, "\tat org.apache.flink") {
		t.Errorf("--full: %d\n%s", code, out)
	}
	code, out, _ = exec("exceptions", "tm-lost", "--rest-url", url, "-o", "json")
	var rep ExceptionReport
	if err := json.Unmarshal([]byte(out), &rep); code != ExitOK || err != nil || len(rep.Entries) == 0 || rep.Job.JobID == "" {
		t.Errorf("json: %d %v\n%s", code, err, out)
	}
	for _, args := range [][]string{{"-o", "csv"}, {"--limit", "0"}, {"--limit", "-1"}} {
		if code, _, _ := exec(append([]string{"exceptions", "tm-lost"}, args...)...); code != ExitUsage {
			t.Errorf("%v: %d", args, code)
		}
		if code, _, _ := exec(append([]string{"checkpoints", "tm-lost"}, args...)...); code != ExitUsage {
			t.Errorf("checkpoints %v: %d", args, code)
		}
	}
}

func TestCheckpointsCommand(t *testing.T) {
	exec, url := restCluster(t)
	code, out, errOut := exec("checkpoints", "tm-lost", "--rest-url", url, "--limit", "5")
	if code != ExitOK || !strings.Contains(out, "Restored from   checkpoint #") {
		t.Fatalf("%d %s\n%s", code, errOut, out)
	}
	code, out, _ = exec("checkpoints", "tm-lost", "--rest-url", url, "-o", "yaml")
	if code != ExitOK || !strings.Contains(out, "restored:") || !strings.Contains(out, "interval:") {
		t.Errorf("yaml: %d\n%s", code, out)
	}
}

func TestRESTRefusedWhenJobManagerDown(t *testing.T) {
	f := kubetest.New(t)
	f.Put("flinkdeployments", kubetest.ReadFixture(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/bad-image/flinkdeployment.json"))
	c := newCluster(t)
	c.add("dev", "default", f)
	code, _, errOut := c.run("exceptions", "bad-image")
	if code != ExitError || !strings.Contains(errOut, "JobManager is not running") || !strings.Contains(errOut, "is ERROR") ||
		!strings.Contains(errOut, "kubectl flink logs default/bad-image --previous") {
		t.Errorf("%d %s", code, errOut)
	}
}

func TestStale(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	ms := func(d time.Duration) flink.Millis { return flink.Millis(now.Add(-d).UnixMilli()) }
	cfg := &flink.CheckpointConfig{Interval: 60_000, Timeout: 600_000}
	cps := func(lastAck time.Duration, failed int) *flink.Checkpoints {
		c := &flink.Checkpoints{}
		c.Counts.Failed = failed
		if lastAck >= 0 {
			c.Latest.Completed = &flink.Checkpoint{LatestAck: ms(lastAck)}
		}
		return c
	}
	for name, tc := range map[string]struct {
		c    *flink.Checkpoints
		cfg  *flink.CheckpointConfig
		want string
	}{
		"fresh":              {cps(time.Minute, 0), cfg, ""},
		"within the timeout": {cps(10*time.Minute, 0), cfg, ""},
		"old":                {cps(45*time.Minute, 3), cfg, "last completed checkpoint is 45m old, interval 60s"},
		"never completed":    {cps(-1, 7), cfg, "no checkpoint has completed; 7 failed"},
		"not started":        {cps(-1, 0), cfg, ""},
		"periodic off":       {cps(45*time.Minute, 0), &flink.CheckpointConfig{Interval: triage.PeriodicOff}, ""},
		"no config":          {cps(45*time.Minute, 0), nil, ""},
	} {
		if got := triage.StaleCheckpoint(tc.c, tc.cfg, now); tc.want == "" && got != "" || !strings.Contains(got, tc.want) {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}
