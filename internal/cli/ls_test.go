package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/profile"
)

const (
	fdFixture   = "../../testdata/fixtures/operator-1.16.1-flink-2.2/flinkdeployment.json"
	podsFixture = "../../testdata/fixtures/operator-1.16.1-flink-2.2/pods.json"
)

// cluster is a kubeconfig whose contexts point at fake API servers.
type cluster struct {
	t          *testing.T
	kubeconfig string
	contexts   map[string]string // context → server
	current    string
}

func newCluster(t *testing.T) *cluster {
	t.Setenv(profile.EnvPath, filepath.Join(t.TempDir(), "absent.yaml"))
	t.Setenv("NO_COLOR", "1")
	return &cluster{t: t, kubeconfig: filepath.Join(t.TempDir(), "config"), contexts: map[string]string{}}
}

// add registers a context served by f, with namespace as its default namespace.
func (c *cluster) add(name, namespace string, f *kubetest.API) {
	c.contexts[name] = f.Start().Host + "|" + namespace
	if c.current == "" {
		c.current = name
	}
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: v1\nkind: Config\ncurrent-context: %s\nusers: [{name: u, user: {token: t}}]\nclusters:\n", c.current)
	for n, v := range c.contexts {
		server, _, _ := strings.Cut(v, "|")
		fmt.Fprintf(&b, "- name: %s\n  cluster: {server: %q}\n", n, server)
	}
	b.WriteString("contexts:\n")
	for n, v := range c.contexts {
		_, ns, _ := strings.Cut(v, "|")
		fmt.Fprintf(&b, "- name: %s\n  context: {cluster: %s, user: u, namespace: %s}\n", n, n, ns)
	}
	if err := os.WriteFile(c.kubeconfig, []byte(b.String()), 0o600); err != nil {
		c.t.Fatal(err)
	}
}

var testNow = time.Now()

func (c *cluster) ls(args ...string) (code int, stdout, stderr string) {
	streams, _, out, errOut := genericiooptions.NewTestIOStreams()
	o := &Options{ConfigFlags: genericclioptions.NewConfigFlags(true), IOStreams: streams, Now: func() time.Time { return testNow }}
	cmd := newRootCommand(o)
	cmd.SetArgs(append([]string{"--kubeconfig", c.kubeconfig, "ls"}, args...))
	code = run(cmd, streams)
	return code, out.String(), errOut.String()
}

// addJobs adds n FlinkDeployments with one ready JobManager and TaskManager pod each.
func addJobs(f *kubetest.API, ns, prefix string, n int, edit func(int, map[string]any)) {
	f.Add("flinkdeployments", fdFixture, ns, prefix, n, edit)
	for _, component := range []string{"jobmanager", "taskmanager"} {
		f.Add("pods", podsFixture, ns, prefix+"-"+component, n, func(i int, o map[string]any) {
			labels := o["metadata"].(map[string]any)["labels"].(map[string]any)
			labels["app"], labels["component"] = fmt.Sprintf("%s-%d", prefix, i), component
		})
	}
}

// healthyAndFailed serves two healthy jobs and one failed job in team-a, and one healthy job in team-b.
func healthyAndFailed(t *testing.T) *kubetest.API {
	f := kubetest.New(t)
	addJobs(f, "team-a", "ok", 2, nil)
	addJobs(f, "team-a", "broken", 1, func(_ int, o map[string]any) {
		st := o["status"].(map[string]any)
		st["lifecycleState"], st["error"] = "FAILED", `{"type":"x.Y","message":"savepoint not found"}`
	})
	addJobs(f, "team-b", "other", 1, nil)
	return f
}

func TestLsDefaultNamespace(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", healthyAndFailed(t))
	code, out, errOut := c.ls()
	if code != ExitOK {
		t.Fatalf("code %d: %s", code, errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "NAME ") || !strings.HasPrefix(lines[1], "broken-0 ") ||
		!strings.Contains(lines[1], "savepoint not found") {
		t.Errorf("table:\n%s", out)
	}
	if !strings.Contains(errOut, "3 jobs: 1 FAILED, 2 OK · 6 pods") {
		t.Errorf("summary on stderr: %q", errOut)
	}
}

func TestLsAllNamespacesFiltersAndExitCode(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", healthyAndFailed(t))
	code, out, _ := c.ls("-A", "--unhealthy", "--exit-code", "--no-headers")
	if code != ExitUnhealthy || strings.TrimSpace(out) == "" || strings.Count(out, "\n") != 1 || !strings.HasPrefix(out, "team-a ") {
		t.Errorf("code %d:\n%s", code, out)
	}
	code, out, _ = c.ls("-A", "-o", "name", "other")
	if code != ExitOK || out != "team-b/other-0\n" {
		t.Errorf("name filter: %d %q", code, out)
	}
	code, out, _ = c.ls("-A", "--health", "ok", "--exit-code", "-o", "name")
	if code != ExitOK || strings.Count(out, "\n") != 3 {
		t.Errorf("ok only: %d %q", code, out)
	}
}

func TestLsJSON(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", healthyAndFailed(t))
	code, out, _ := c.ls("-n", "team-b", "-o", "json")
	var l struct {
		Kind  string `json:"kind"`
		Items []struct {
			Name, Namespace, Health string
			TaskManagers            *struct{ Ready, Desired int }
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &l); code != ExitOK || err != nil {
		t.Fatalf("code %d, %v: %s", code, err, out)
	}
	if l.Kind != "JobList" || len(l.Items) != 1 || l.Items[0].Namespace != "team-b" || l.Items[0].Health != "OK" ||
		l.Items[0].TaskManagers == nil || l.Items[0].TaskManagers.Ready != 1 {
		t.Errorf("%+v", l)
	}
}

func TestLsForbiddenFallsBack(t *testing.T) {
	f := healthyAndFailed(t)
	f.Forbidden["flinkdeployments/"] = true
	c := newCluster(t)
	c.add("dev", "team-b", f)
	code, out, errOut := c.ls("-A")
	if code != ExitOK || !strings.Contains(out, "other-0") || strings.Contains(out, "broken") {
		t.Errorf("code %d:\n%s", code, out)
	}
	if !strings.Contains(errOut, "note: cannot list Flink resources at cluster scope in context dev; showing namespace team-b") {
		t.Errorf("note: %q", errOut)
	}
}

func TestLsProfileScopes(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", healthyAndFailed(t))
	down := kubetest.New(t)
	down.NotFound = true
	c.add("prd", "team-x", down)
	prof := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(prof, []byte(`apiVersion: kubectl-flink/v1alpha1
profiles:
  - name: jumphost
    match: ["*"]
    scopes:
      - context: dev
        namespaces: [team-a, team-b]
      - context: prd
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(profile.EnvPath, prof)
	code, out, _ := c.ls()
	if code != ExitError {
		t.Errorf("a failed scope must fail the exit code, got %d", code)
	}
	if !strings.HasPrefix(out, "CONTEXT   NAMESPACE") || !strings.Contains(out, "prd ") || !strings.Contains(out, "not installed") ||
		strings.Count(out, "dev ") != 4 {
		t.Errorf("table:\n%s", out)
	}
	code, out, _ = c.ls("--context", "dev", "-o", "name")
	if code != ExitOK || strings.Count(out, "\n") != 4 {
		t.Errorf("--context narrows to dev: %d\n%s", code, out)
	}
}

func TestLsErrors(t *testing.T) {
	c := newCluster(t)
	down := kubetest.New(t)
	down.NotFound = true
	c.add("dev", "team-a", down)
	if code, _, errOut := c.ls(); code != ExitError || !strings.Contains(errOut, "operator's resources are not installed") {
		t.Errorf("single failed scope: %d %q", code, errOut)
	}
	for _, args := range [][]string{{"-o", "csv"}, {"--sort", "colour"}, {"--health", "sad"}, {"-w", "-o", "name"}, {"a", "b"}} {
		if code, _, _ := c.ls(args...); code != ExitUsage {
			t.Errorf("%v: code %d, want usage", args, code)
		}
	}
}

// syncBuffer is a bytes.Buffer safe for a writer and a reader in different goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitFor(t *testing.T, b *syncBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(b.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("no %q in:\n%s", want, b.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLsWatch(t *testing.T) {
	f := healthyAndFailed(t)
	c := newCluster(t)
	c.add("dev", "team-b", f)
	var out, errOut syncBuffer
	streams := genericiooptions.IOStreams{In: strings.NewReader(""), Out: &out, ErrOut: &errOut}
	o := &Options{ConfigFlags: genericclioptions.NewConfigFlags(true), IOStreams: streams, Now: func() time.Time { return testNow }}
	cmd := newRootCommand(o)
	cmd.SetArgs([]string{"--kubeconfig", c.kubeconfig, "ls", "-w"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int)
	cmd.SetContext(ctx)
	go func() { done <- run(cmd, streams) }()

	waitFor(t, &out, "other-0")
	f.Add("flinkdeployments", fdFixture, "team-b", "other", 1, func(_ int, o map[string]any) {
		o["status"].(map[string]any)["lifecycleState"] = "FAILED"
	})
	waitFor(t, &out, "FAILED")
	cancel()
	if code := <-done; code != ExitOK {
		t.Errorf("code %d: %s", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "NAME") || !strings.HasPrefix(lines[2], "other-0 ") {
		t.Errorf("want header, initial row and one changed row:\n%s", out.String())
	}
}

func TestReportWatchErrors(t *testing.T) {
	streams, _, _, errOut := genericiooptions.NewTestIOStreams()
	lo := &lsOptions{Options: &Options{IOStreams: streams}}
	last := map[string]string{}
	failing := []fleet.Scope{{Scope: kube.Scope{Context: "dev"}, WatchErr: errors.New("connection reset")}}
	lo.reportWatchErrors(failing, last)
	lo.reportWatchErrors(failing, last)
	lo.reportWatchErrors([]fleet.Scope{{Scope: kube.Scope{Context: "dev"}}}, last)
	if got := errOut.String(); got != "warning: context dev: connection reset; rows may be stale\ncontext dev: watching again\n" {
		t.Errorf("%q", got)
	}
}

// top needs a terminal, and checks its flags first.
func TestTopUsage(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", kubetest.New(t))
	for _, args := range [][]string{{}, {"--sort", "colour"}, {"--health", "sad"}} {
		streams, _, _, errOut := genericiooptions.NewTestIOStreams()
		o := &Options{ConfigFlags: genericclioptions.NewConfigFlags(true), IOStreams: streams}
		cmd := newRootCommand(o)
		cmd.SetArgs(append([]string{"--kubeconfig", c.kubeconfig, "top"}, args...))
		if code := run(cmd, streams); code != ExitUsage {
			t.Errorf("%v: code %d, want usage", args, code)
		}
		if len(args) == 0 && !strings.Contains(errOut.String(), "top needs a terminal") {
			t.Errorf("no terminal: %q", errOut.String())
		}
	}
}

// top's loader keeps one port-forward pool per context for the session, and knows only the contexts it read.
func TestTopLoader(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", kubetest.New(t))
	streams, _, _, _ := genericiooptions.NewTestIOStreams()
	o := &Options{ConfigFlags: genericclioptions.NewConfigFlags(true), IOStreams: streams}
	kc := c.kubeconfig
	o.ConfigFlags.KubeConfig = &kc
	srcs, prof, err := o.sources(false)
	if err != nil {
		t.Fatal(err)
	}
	l, err := newTopLoader(o, srcs, prof)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	job := model.Job{Context: "dev", Namespace: "team-a", Name: "orders", Kind: model.KindDeployment}
	tg, err := l.target(&job)
	if err != nil {
		t.Fatal(err)
	}
	if first := l.pool(tg); l.pool(tg) != first {
		t.Error("a pool per call, not per context")
	}
	job.Context = "gone"
	if _, err := l.Detail(t.Context(), job); err == nil {
		t.Error("unknown context collected")
	}
	if _, err := l.Log(t.Context(), job, 10); err == nil {
		t.Error("unknown context read")
	}
}
