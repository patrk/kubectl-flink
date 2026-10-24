package fleet_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericclioptions"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
)

func status(o map[string]any) map[string]any { return o["status"].(map[string]any) }

func TestLoadScopes(t *testing.T) {
	dev := kubetest.New(t)
	dev.Add("flinkdeployments", fdFixture, "team-a", "job", 2, nil)
	dev.Add("pods", podsFixture, "team-a", "pod", 1, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["labels"].(map[string]any)["app"] = "job-0"
	})
	prd := kubetest.New(t)
	prd.NotFound = true
	broken := fleet.Source{Scope: kube.Scope{Context: "gone"}, Err: errors.New("context gone not found in kubeconfig")}

	scopes := fleet.Load(context.Background(), []fleet.Source{
		source(t, dev, kube.Scope{Context: "dev"}), source(t, prd, kube.Scope{Context: "prd"}), broken,
	}, "")
	if len(scopes) != 3 || scopes[0].Err != nil || len(scopes[0].Jobs) != 2 {
		t.Fatalf("dev: %+v", scopes[0])
	}
	if !errors.Is(scopes[1].Err, kube.ErrNoOperator) || scopes[2].Err == nil {
		t.Errorf("errors: %v, %v", scopes[1].Err, scopes[2].Err)
	}
	j0, j1 := scopes[0].Jobs[0], scopes[0].Jobs[1]
	// The pod fixture is a JobManager pod.
	if j0.Context != "dev" || j0.Pods == nil || j0.Pods.JM.Total != 1 || j1.Pods == nil || j1.Pods.JM.Total != 0 {
		t.Errorf("pods attached: %+v %+v", j0.Pods, j1.Pods)
	}
}

func TestLoadLabelSelector(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "pay", 2, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["labels"] = map[string]any{"team": "payments"}
	})
	f.Add("flinkdeployments", fdFixture, "team-a", "other", 3, nil)
	scopes := fleet.Load(context.Background(), []fleet.Source{source(t, f, kube.Scope{Context: "dev"})}, "team=payments")
	if n := len(scopes[0].Jobs); n != 2 {
		t.Errorf("%d jobs, want the 2 labelled ones", n)
	}
}

func TestLoadPodsUnread(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 1, nil)
	f.Forbidden["pods/team-a"] = true
	s := fleet.Load(context.Background(), []fleet.Source{source(t, f, kube.Scope{Context: "dev"})}, "")[0]
	if s.Err != nil || strings.Join(s.PodsUnread, ",") != "team-a" || s.Jobs[0].Pods != nil {
		t.Errorf("%+v", s)
	}
}

func rowsOf(jobs ...model.Job) []fleet.Row {
	scopes := []fleet.Scope{{Jobs: jobs}}
	return fleet.Rows(scopes, time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC), model.DefaultThresholds())
}

func job(ns, name, lifecycle string) model.Job {
	at := time.Date(2026, 10, 24, 11, 0, 0, 0, time.UTC)
	return model.Job{Kind: model.KindDeployment, Context: "dev", Namespace: ns, Name: name, Lifecycle: lifecycle,
		JobState: "RUNNING", JMStatus: "READY", ReconciledAt: at, JobStateSince: at, Created: at, SpecJobState: "running"}
}

func names(rows []fleet.Row) string {
	b := make([]string, 0, len(rows))
	for _, r := range rows {
		b = append(b, r.Job.Name)
	}
	return strings.Join(b, ",")
}

func TestFilterAndSort(t *testing.T) {
	rows := rowsOf(job("b", "ok-1", "STABLE"), job("a", "failed", "FAILED"), job("a", "ok-2", "STABLE"), job("a", "rolled", "ROLLED_BACK"))
	if err := fleet.Sort(rows, "health"); err != nil || names(rows) != "failed,rolled,ok-2,ok-1" {
		t.Errorf("health: %s %v", names(rows), err)
	}
	if err := fleet.Sort(rows, "name"); err != nil || names(rows) != "failed,ok-1,ok-2,rolled" {
		t.Errorf("name: %s", names(rows))
	}
	if err := fleet.Sort(rows, "namespace"); err != nil || names(rows) != "failed,ok-2,rolled,ok-1" {
		t.Errorf("namespace: %s", names(rows))
	}
	if err := fleet.Sort(rows, "colour"); err == nil {
		t.Error("want error for unknown key")
	}
	f := fleet.Filter{Health: []model.Health{model.Degraded, model.Failed}}
	if got := names(f.Apply(rows)); got != "failed,rolled" {
		t.Errorf("health filter: %s", got)
	}
	f = fleet.Filter{Text: "a/ok"}
	if got := names(f.Apply(rows)); got != "ok-2" {
		t.Errorf("text filter: %s", got)
	}
}

func TestFind(t *testing.T) {
	jobs := []*model.Job{
		{Namespace: "a", Name: "orders"}, {Namespace: "b", Name: "orders"}, {Namespace: "a", Name: "orders-replay"},
		{Namespace: "a", Name: "payments"},
	}
	for arg, want := range map[string]string{"a/orders": "a/orders", "pay": "a/payments", "b/ord": "b/orders"} {
		if j, err := fleet.Find(jobs, arg); err != nil || j.Key() != want {
			t.Errorf("%s: %v %v", arg, j, err)
		}
	}
	var amb *fleet.AmbiguousError
	if _, err := fleet.Find(jobs, "orders"); !errors.As(err, &amb) || len(amb.Candidates) != 2 {
		t.Errorf("exact name in two namespaces: %v", err)
	}
	if _, err := fleet.Find(jobs, "ord"); !errors.As(err, &amb) || len(amb.Candidates) != 3 || !strings.Contains(err.Error(), "a/orders-replay") {
		t.Errorf("substring: %v", err)
	}
	if _, err := fleet.Find(jobs, "nothing"); !errors.Is(err, fleet.ErrNoMatch) {
		t.Errorf("none: %v", err)
	}
}

func TestSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	cfg := "apiVersion: v1\nkind: Config\ncurrent-context: dev\nusers: [{name: u, user: {token: t}}]\n" +
		"clusters: [{name: dev, cluster: {server: 'https://dev.example'}}]\n" +
		"contexts: [{name: dev, context: {cluster: dev, user: u, namespace: team-a}}]\n"
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	flags := genericclioptions.NewConfigFlags(false)
	flags.KubeConfig = &path
	cs, err := kube.NewContexts(flags)
	if err != nil {
		t.Fatal(err)
	}
	srcs := fleet.Sources(cs, []kube.Scope{{Context: "dev"}, {Context: "missing"}})
	if srcs[0].Err != nil || srcs[0].Client == nil || srcs[0].ContextNamespace != "team-a" || srcs[1].Err == nil {
		t.Errorf("%+v", srcs)
	}
}

func TestSortByAge(t *testing.T) {
	older, newer := job("a", "older", "STABLE"), job("a", "newer", "STABLE")
	older.Created, older.JobStateSince = older.Created.Add(-time.Hour), older.JobStateSince.Add(-time.Hour)
	newer.JobStateSince = time.Time{} // falls back to the reconcile time
	rows := rowsOf(newer, older)
	for _, key := range []string{"age", "state-age"} {
		if err := fleet.Sort(rows, key); err != nil || names(rows) != "older,newer" {
			t.Errorf("%s: %s %v", key, names(rows), err)
		}
	}
}
