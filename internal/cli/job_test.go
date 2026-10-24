package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/profile"
)

func (c *cluster) options() *Options {
	streams, _, _, _ := genericiooptions.NewTestIOStreams()
	flags := genericclioptions.NewConfigFlags(true)
	flags.KubeConfig = &c.kubeconfig
	return &Options{ConfigFlags: flags, IOStreams: streams, Now: func() time.Time { return testNow }}
}

func (c *cluster) run(args ...string) (code int, stdout, stderr string) {
	streams, _, out, errOut := genericiooptions.NewTestIOStreams()
	o := &Options{ConfigFlags: genericclioptions.NewConfigFlags(true), IOStreams: streams, Now: func() time.Time { return testNow }}
	cmd := newRootCommand(o)
	cmd.SetArgs(append([]string{"--kubeconfig", c.kubeconfig}, args...))
	code = run(cmd, streams)
	return code, out.String(), errOut.String()
}

func TestResolveJob(t *testing.T) {
	f := healthyAndFailed(t)
	f.Add("flinkdeployments", fdFixture, "team-b", "orders", 1, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["labels"] = map[string]any{"use-case": "uc-42"}
	})
	f.Add("flinksessionjobs", fdFixture, "team-b", "enrich", 1, func(_ int, o map[string]any) {
		o["spec"] = map[string]any{"deploymentName": "session"}
	})
	c := newCluster(t)
	c.add("dev", "team-a", f)
	ctx := context.Background()

	for arg, want := range map[string]string{
		"team-a/ok-1": "team-a/ok-1", "broken-0": "team-a/broken-0", "brok": "team-a/broken-0",
	} {
		tgt, err := c.options().resolveJob(ctx, arg, false)
		if err != nil || tgt.job.Key() != want {
			t.Errorf("%s: %v %v", arg, tgt, err)
		}
	}
	// Outside the context namespace only with -A, or with namespace/name.
	if _, err := c.options().resolveJob(ctx, "uc-42", false); !errors.Is(err, fleet.ErrNoMatch) {
		t.Errorf("uc-42 without -A: %v", err)
	}
	if tgt, err := c.options().resolveJob(ctx, "uc-42", true); err != nil || tgt.job.Name != "orders-0" {
		t.Errorf("by ID label with -A: %v %v", tgt, err)
	}
	tgt, err := c.options().resolveJob(ctx, "team-b/enrich-0", false)
	if err != nil || tgt.cluster() != "session" || !strings.Contains(tgt.String(), "FlinkSessionJob team-b/enrich-0") {
		t.Errorf("session job: %v %v", tgt, err)
	}
	var ee *exitError
	if _, err := c.options().resolveJob(ctx, "ok", false); !errors.As(err, &ee) || ee.code != ExitAmbiguous ||
		!strings.Contains(err.Error(), "team-a/ok-0") {
		t.Errorf("ambiguous: %v", err)
	}
	if _, err := c.options().resolveJob(ctx, "team-a/nope", false); !errors.Is(err, fleet.ErrNoMatch) {
		t.Errorf("missing: %v", err)
	}
	// namespace/part-of-a-name falls back to the substring match.
	if tgt, err := c.options().resolveJob(ctx, "team-a/brok", false); err != nil || tgt.job.Name != "broken-0" {
		t.Errorf("ns/substring: %v %v", tgt, err)
	}
}

func TestResolveJobReportsUnreadableScopes(t *testing.T) {
	f := healthyAndFailed(t)
	down := kubetest.New(t)
	down.Forbidden["flinkdeployments/team-x"] = true
	c := newCluster(t)
	c.add("dev", "team-a", f)
	c.add("prd", "team-x", down)
	prof := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(prof, []byte("apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: p, match: ['*'], scopes: [{context: dev}, {context: prd}]}]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(profile.EnvPath, prof)
	ctx := context.Background()
	// Found in dev: prd's failure does not matter.
	if tgt, err := c.options().resolveJob(ctx, "team-a/broken-0", false); err != nil || tgt.job.Context != "dev" {
		t.Errorf("found despite prd: %v %v", tgt, err)
	}
	if tgt, err := c.options().resolveJob(ctx, "broken", false); err != nil || tgt.job.Name != "broken-0" {
		t.Errorf("listed despite prd: %v %v", tgt, err)
	}
	// Not found: prd's failure is named, so it does not read as a plain "no match".
	_, err := c.options().resolveJob(ctx, "team-x/orders", false)
	if !errors.Is(err, fleet.ErrNoMatch) || !strings.Contains(err.Error(), "not every scope could be read") || !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("not found: %v", err)
	}
}

func TestResolveJobIDLabelsFromProfile(t *testing.T) {
	f := kubetest.New(t)
	addJobs(f, "team-a", "orders", 1, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["labels"] = map[string]any{"use-case": "uc-1", "env": "prod"}
	})
	addJobs(f, "team-a", "payments", 1, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["labels"] = map[string]any{"env": "prod"}
	})
	c := newCluster(t)
	c.add("dev", "team-a", f)
	prof := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(prof, []byte("apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: p, match: ['*'], idLabels: [use-case]}]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(profile.EnvPath, prof)
	if tgt, err := c.options().resolveJob(context.Background(), "uc-1", false); err != nil || tgt.job.Name != "orders-0" {
		t.Errorf("uc-1: %v %v", tgt, err)
	}
	if _, err := c.options().resolveJob(context.Background(), "prod", false); !errors.Is(err, fleet.ErrNoMatch) {
		t.Errorf("env is not an ID label: %v", err)
	}
	code, out, _ := c.ls("-o", "name", "uc-1")
	if code != ExitOK || out != "team-a/orders-0\n" {
		t.Errorf("ls by ID: %d %q", code, out)
	}
}

func TestLogs(t *testing.T) {
	f := healthyAndFailed(t)
	f.Logs = map[string]string{
		"team-a/broken-jobmanager-0":  "one\ntwo\nthree\n",
		"team-a/broken-taskmanager-0": "tm line\n",
	}
	c := newCluster(t)
	c.add("dev", "team-a", f)
	code, out, errOut := c.run("logs", "broken-0", "--tail", "2")
	if code != ExitOK || out != "two\nthree\n" {
		t.Errorf("jobmanager: %d %q %s", code, out, errOut)
	}
	code, out, _ = c.run("logs", "team-a/broken-0", "--taskmanagers")
	if code != ExitOK || out != "tm line\n" {
		t.Errorf("taskmanager: %d %q", code, out)
	}
	f.Logs["team-a/broken-taskmanager-1"] = "second tm\n"
	f.Add("pods", podsFixture, "team-a", "broken-taskmanager", 2, func(_ int, o map[string]any) {
		labels := o["metadata"].(map[string]any)["labels"].(map[string]any)
		labels["app"], labels["component"] = "broken-0", "taskmanager"
	})
	code, out, _ = c.run("logs", "broken-0", "--taskmanagers")
	if code != ExitOK || !strings.Contains(out, "[broken-taskmanager-0] tm line\n") || !strings.Contains(out, "[broken-taskmanager-1] second tm\n") {
		t.Errorf("two taskmanagers: %d %q", code, out)
	}
	// One TaskManager without a log does not cut off the other.
	delete(f.Logs, "team-a/broken-taskmanager-1")
	code, out, errOut = c.run("logs", "broken-0", "--taskmanagers")
	if code != ExitError || !strings.Contains(out, "[broken-taskmanager-0] tm line\n") ||
		!strings.Contains(errOut, "warning: log of broken-taskmanager-1") || !strings.Contains(errOut, "1 of 2 logs could not be read") {
		t.Errorf("one log missing: %d %q %q", code, out, errOut)
	}
	if code, _, errOut := c.run("logs", "ok"); code != ExitAmbiguous {
		t.Errorf("ambiguous: %d %s", code, errOut)
	}
}
