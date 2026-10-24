package kube_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/cache"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
)

const (
	fdFixture   = "../../testdata/fixtures/operator-1.16.1-flink-2.2/flinkdeployment.json"
	podsFixture = "../../testdata/fixtures/operator-1.16.1-flink-2.2/pods.json"
)

func client(t *testing.T, f *kubetest.API) *kube.Client {
	t.Helper()
	c, err := kube.NewClient(f.Start())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func isList(r kubetest.Request) bool {
	return r.Query.Get("watch") == "" && r.Query.Get("limit") != "1"
}

func TestListPaginates(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 600, nil)
	obj, err := client(t, f).List(context.Background(), kube.Deployments, "team-a", "")
	if err != nil {
		t.Fatal(err)
	}
	l := obj.(*kube.FlinkDeploymentList)
	if len(l.Items) != 600 || l.Items[599].Name != "job-599" || l.Items[0].Status.JobStatus.State != "RUNNING" {
		t.Fatalf("got %d items, last %q", len(l.Items), l.Items[len(l.Items)-1].Name)
	}
	if l.Continue != "" || l.RemainingItemCount != nil {
		t.Errorf("merged list looks partial: continue %q", l.Continue)
	}
	if n := f.Count(func(r kubetest.Request) bool { return r.Query.Get("limit") == "250" }); n != 3 {
		t.Errorf("%d pages of 250, want 3", n)
	}
	if _, ok := l.Items[0].Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Error("last-applied-configuration kept")
	}
}

func TestListPaginatesEveryKind(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinksessionjobs", fdFixture, "team-a", "sj", 260, nil)
	f.Add("pods", podsFixture, "team-a", "pod", 260, nil)
	c := client(t, f)
	sjs, err := c.List(context.Background(), kube.SessionJobs, "", "")
	if err != nil || len(sjs.(*kube.FlinkSessionJobList).Items) != 260 {
		t.Fatalf("session jobs: %v", err)
	}
	pods, err := c.List(context.Background(), kube.Pods, "", kube.FlinkPodSelector)
	if err != nil || len(pods.(*kube.PodList).Items) != 260 {
		t.Fatalf("pods: %v", err)
	}
}

func TestListPodsDecodesAndSelects(t *testing.T) {
	f := kubetest.New(t)
	f.Add("pods", podsFixture, "team-a", "pod", 3, nil)
	f.Add("pods", podsFixture, "team-a", "other", 2, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["labels"] = map[string]any{"app": "unrelated"}
	})
	f.Add("pods", podsFixture, "team-a", "standalone", 1, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["labels"].(map[string]any)["type"] = "flink-standalone-kubernetes"
	})
	obj, err := client(t, f).List(context.Background(), kube.Pods, "team-a", kube.FlinkPodSelector)
	if err != nil {
		t.Fatal(err)
	}
	l := obj.(*kube.PodList)
	if len(l.Items) != 4 {
		t.Fatalf("%d pods, want the 3 native and 1 standalone Flink pods", len(l.Items))
	}
	p := l.Items[0]
	cs := p.Status.ContainerStatuses
	if p.Status.Phase != "Running" || len(cs) != 1 || !cs[0].Ready || p.Labels[kube.LabelComponent] == "" {
		t.Errorf("pod not decoded: %+v", p)
	}
	if q := p.Spec.Containers[0].Resources.Requests["memory"]; q.String() != "1Gi" {
		t.Errorf("memory request %s", q.String())
	}
}

func syncInformer(t *testing.T, inf cache.SharedIndexInformer) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go inf.RunWithContext(ctx)
	sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
	defer scancel()
	if !cache.WaitForCacheSync(sctx.Done(), inf.HasSynced) {
		cancel()
		t.Fatal("informer did not sync")
	}
	return cancel
}

func TestInformerUsesWatchList(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 5, nil)
	inf := client(t, f).Informer(kube.Deployments, "team-a", "", nil)
	cancel := syncInformer(t, inf)
	defer cancel()

	if n := len(inf.GetStore().List()); n != 5 {
		t.Fatalf("%d cached, want 5", n)
	}
	if f.Count(func(r kubetest.Request) bool { return r.Query.Get("sendInitialEvents") == "true" }) == 0 {
		t.Error("informer did not ask for initial events")
	}
	if n := f.Count(isList); n != 0 {
		t.Errorf("informer issued %d LIST requests; WatchList should replace them: %v", n, f.Requests())
	}

	updated := make(chan *kube.FlinkDeployment, 1)
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{UpdateFunc: func(_, obj any) {
		updated <- obj.(*kube.FlinkDeployment)
	}})
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 3, func(i int, o map[string]any) {
		if i == 2 {
			o["status"].(map[string]any)["lifecycleState"] = "FAILED"
		}
	})
	timeout := time.After(10 * time.Second)
	for {
		select {
		case fd := <-updated:
			if fd.Name == "job-2" {
				if fd.Status.LifecycleState != "FAILED" {
					t.Errorf("job-2 lifecycle %s", fd.Status.LifecycleState)
				}
				return
			}
		case <-timeout:
			t.Fatal("no update for job-2")
		}
	}
}

func TestInformerFallsBackToList(t *testing.T) {
	f := kubetest.New(t)
	f.NoWatchList = true
	f.Add("pods", podsFixture, "team-a", "pod", 4, nil)
	inf := client(t, f).Informer(kube.Pods, "team-a", kube.FlinkPodSelector, nil)
	cancel := syncInformer(t, inf)
	defer cancel()

	if n := len(inf.GetStore().List()); n != 4 {
		t.Fatalf("%d cached, want 4", n)
	}
	if f.Count(isList) == 0 {
		t.Error("no LIST after WatchList was rejected")
	}
}

func TestResolve(t *testing.T) {
	ctx := context.Background()
	scope := kube.Scope{Context: "dev", AllNamespaces: true}
	withNS := scope
	withNS.Namespaces = []string{"team-a", "team-b"}

	p, err := client(t, kubetest.New(t)).Resolve(ctx, scope, "default")
	if err != nil || len(p.Read) != 1 || p.Read[0] != "" || p.Note != "" {
		t.Errorf("cluster scope allowed: %+v %v", p, err)
	}

	for _, forbidden := range []string{"flinkdeployments/", "flinksessionjobs/"} {
		f := kubetest.New(t)
		f.Forbidden[forbidden] = true
		c := client(t, f)
		p, err = c.Resolve(ctx, withNS, "default")
		if err != nil || strings.Join(p.Read, ",") != "team-a,team-b" || !strings.Contains(p.Note, "team-a, team-b") {
			t.Errorf("%s forbidden, scope namespaces: %+v %v", forbidden, p, err)
		}
		p, err = c.Resolve(ctx, scope, "default")
		if err != nil || strings.Join(p.Read, ",") != "default" || !strings.Contains(p.Note, "cluster scope") {
			t.Errorf("%s forbidden, context namespace: %+v %v", forbidden, p, err)
		}
	}

	// The API server's message survives, not a generic "unknown".
	f := kubetest.New(t)
	f.Forbidden["flinkdeployments/team-x"] = true
	if _, err := client(t, f).Get(ctx, kube.Deployments, "team-x", "orders"); !strings.Contains(err.Error(), "flinkdeployments is forbidden") {
		t.Errorf("forbidden message: %v", err)
	}
	f = kubetest.New(t)
	f.NotFound = true
	if _, err := client(t, f).Resolve(ctx, scope, "default"); !errors.Is(err, kube.ErrNoOperator) {
		t.Errorf("not found: %v", err)
	}

	p, err = client(t, kubetest.New(t)).Resolve(ctx, kube.Scope{Context: "dev"}, "default")
	if err != nil || strings.Join(p.Read, ",") != "default" {
		t.Errorf("no -A: %+v %v", p, err)
	}
	// "" among the namespaces stands for the context namespace, as a merged profile scope produces.
	p, err = client(t, kubetest.New(t)).Resolve(ctx, kube.Scope{Context: "dev", Namespaces: []string{"", "a", "default"}}, "default")
	if err != nil || strings.Join(p.Read, ",") != "default,a" {
		t.Errorf("context namespace marker: %+v %v", p, err)
	}
}

func TestLoad(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "a", 3, nil)
	f.Add("flinkdeployments", fdFixture, "team-b", "b", 2, nil)
	f.Add("pods", podsFixture, "team-a", "pa", 2, nil)
	f.Add("pods", podsFixture, "team-b", "pb", 2, nil)
	f.Forbidden["pods/team-b"] = true
	c := client(t, f)
	plan := kube.Plan{Scope: kube.Scope{Context: "dev"}, Read: []string{"team-a", "team-b"}}

	s, err := c.Load(context.Background(), plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Deployments) != 5 || s.Deployments[0].Namespace != "team-a" || s.Deployments[4].Namespace != "team-b" {
		t.Errorf("%d deployments", len(s.Deployments))
	}
	if len(s.Pods) != 2 || s.PodsErr["team-b"] == nil || !s.PodsRead("team-a") || s.PodsRead("team-b") {
		t.Errorf("pods %d, errors %v; want team-a read and team-b not", len(s.Pods), s.PodsErr)
	}

	f.Set(func() { f.Forbidden["flinksessionjobs/team-b"] = true })
	if _, err = c.Load(context.Background(), plan, ""); err == nil || !strings.Contains(err.Error(), "namespace team-b") {
		t.Errorf("forbidden session jobs: %v", err)
	}

	f.Set(func() { f.NotFound = true })
	if _, err := c.Load(context.Background(), kube.Plan{Read: []string{"team-a"}}, ""); !errors.Is(err, kube.ErrNoOperator) {
		t.Errorf("not found: %v", err)
	}
}
