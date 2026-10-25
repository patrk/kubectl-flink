package kube_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// A page that cannot be decoded fails the list, whether pages after it were read or not.
func TestListPageDecodeError(t *testing.T) {
	for _, bad := range []int{100, 300, 599} {
		f := kubetest.New(t)
		f.Add("flinkdeployments", fdFixture, "team-a", "job", 600, func(i int, o map[string]any) {
			if i == bad {
				o["spec"] = "not an object"
			}
		})
		_, err := client(t, f).List(context.Background(), kube.Deployments, "team-a", "")
		if err == nil || !strings.Contains(err.Error(), "decode flinkdeployments") {
			t.Errorf("bad item %d: %v", bad, err)
		}
	}
	// The page after a bad one fails to be read: the decode error is still the one reported.
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 300, func(i int, o map[string]any) {
		if i == 0 {
			o["spec"] = "not an object"
		}
	})
	f.ForbidContinued = true
	if _, err := client(t, f).List(context.Background(), kube.Deployments, "team-a", ""); err == nil || !strings.Contains(err.Error(), "decode flinkdeployments") {
		t.Errorf("decode error hidden by the next page: %v", err)
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

	s, err := c.Load(context.Background(), plan, kube.Query{})
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
	if _, err = c.Load(context.Background(), plan, kube.Query{}); err == nil || !strings.Contains(err.Error(), "namespace team-b") {
		t.Errorf("forbidden session jobs: %v", err)
	}

	f.Set(func() { f.NotFound = true })
	if _, err := c.Load(context.Background(), kube.Plan{Read: []string{"team-a"}}, kube.Query{}); !errors.Is(err, kube.ErrNoOperator) {
		t.Errorf("not found: %v", err)
	}
}

func TestLoadMatch(t *testing.T) {
	f := kubetest.New(t)
	label := func(k, v string) func(int, map[string]any) {
		return func(i int, o map[string]any) {
			md := o["metadata"].(map[string]any)
			l, _ := md["labels"].(map[string]any)
			if l == nil {
				l = map[string]any{}
			}
			l[k] = fmt.Sprint(v, i)
			md["labels"] = l
		}
	}
	f.Add("flinkdeployments", fdFixture, "team-a", "a", 3, label("use-case", "7"))
	f.Add("flinkdeployments", fdFixture, "team-b", "b", 2, label("uc", "7"))
	f.Add("pods", podsFixture, "team-a", "pa", 3, label("app", "a-"))
	f.Add("pods", podsFixture, "team-b", "pb", 2, label("app", "b-"))
	c := client(t, f)
	plan := kube.Plan{Scope: kube.Scope{Context: "dev", AllNamespaces: true}, Read: []string{""}}
	load := func(m kube.Match) *kube.Snapshot {
		t.Helper()
		s, err := c.Load(context.Background(), plan, kube.Query{Match: &m})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	names := func(s *kube.Snapshot) string {
		n := make([]string, 0, len(s.Deployments))
		for _, d := range s.Deployments {
			n = append(n, d.Namespace+"/"+d.Name)
		}
		slices.Sort(n)
		return strings.Join(n, " ")
	}

	// a-1 carries use-case=71 and b-1 uc=71: the same ID under either key, found once each.
	s := load(kube.Match{Name: "71", Labels: []string{"use-case", "uc"}, Value: "71"})
	if got := names(s); got != "team-a/a-1 team-b/b-1" {
		t.Errorf("by ID: %s", got)
	}
	// Only the pods of the clusters found are read.
	pods := make([]string, 0, len(s.Pods))
	for _, p := range s.Pods {
		pods = append(pods, p.Name)
	}
	slices.Sort(pods)
	if strings.Join(pods, " ") != "pa-1 pb-1" {
		t.Errorf("pods %v", pods)
	}
	if got := names(load(kube.Match{Name: "a-2", Labels: []string{"use-case"}, Value: "a-2"})); got != "team-a/a-2" {
		t.Errorf("by name: %s", got)
	}
	// A value no label can hold reads nothing by label, and nothing at all is listed unfiltered.
	if got := names(load(kube.Match{Labels: []string{"use-case"}, Value: "not a label value"})); got != "" {
		t.Errorf("invalid value: %s", got)
	}
	unfiltered := f.Count(func(r kubetest.Request) bool {
		return strings.HasSuffix(r.Path, "/flinkdeployments") && r.Query.Get("labelSelector") == "" && r.Query.Get("fieldSelector") == ""
	})
	if unfiltered != 0 {
		t.Errorf("%d unfiltered deployment lists", unfiltered)
	}
}

func TestLoadMatchManyNamespacesAndLongNames(t *testing.T) {
	f := kubetest.New(t)
	long := strings.Repeat("x", 70)
	read := make([]string, 0, 9)
	for i := range 9 {
		ns := fmt.Sprintf("team-%d", i)
		read = append(read, ns)
		f.Add("flinkdeployments", fdFixture, ns, "job", 2, func(j int, o map[string]any) {
			o["metadata"].(map[string]any)["labels"] = map[string]any{"use-case": fmt.Sprint(i*10 + j)}
		})
	}
	f.Add("flinkdeployments", fdFixture, "team-0", long, 1, func(_ int, o map[string]any) {
		o["metadata"].(map[string]any)["name"] = long
		o["metadata"].(map[string]any)["labels"] = map[string]any{"use-case": "1"}
	})
	f.Add("pods", podsFixture, "team-0", "p", 1, nil)
	c := client(t, f)
	ctx := context.Background()
	m := &kube.Match{Labels: []string{"use-case"}, Value: "1"}

	// Nine namespaces read one by one: the full load, matched as the API server would.
	s, err := c.Load(ctx, kube.Plan{Read: read}, kube.Query{Match: m})
	if err != nil || len(s.Deployments) != 2 || s.Deployments[0].Name != "job-1" || s.Deployments[1].Name != long {
		t.Fatalf("many namespaces: %v %d", err, len(s.Deployments))
	}
	labelled := f.Count(func(r kubetest.Request) bool { return strings.Contains(r.Query.Get("labelSelector"), "use-case") })
	if labelled != 0 {
		t.Errorf("%d label lists for nine namespaces", labelled)
	}
	// Fewer: by the API server. A name too long for a label value reads every Flink pod of its namespace.
	s, err = c.Load(ctx, kube.Plan{Read: read[:1]}, kube.Query{Match: m})
	if err != nil || len(s.Deployments) != 2 || len(s.Pods) != 1 || s.PodsErr != nil {
		t.Errorf("long name: %v, %d jobs, %d pods, %v", err, len(s.Deployments), len(s.Pods), s.PodsErr)
	}
	if s.Deployments[0].Name != "job-1" {
		t.Errorf("order: %s first", s.Deployments[0].Name)
	}
}

func TestContinueToken(t *testing.T) {
	for body, want := range map[string]string{
		`{"kind":"FlinkDeploymentList","apiVersion":"v1","metadata":{"continue":"abc"},"items":[{"x":1}]}`: "abc",
		`{"items":[{"metadata":{"continue":"not this"}}],"metadata":{"continue":"def"}}`:                   "def",
		`{"metadata":{}}`: "",
		`{}`:              "",
	} {
		if got, err := kube.ContinueToken([]byte(body)); err != nil || got != want {
			t.Errorf("%s: %q %v, want %q", body, got, err, want)
		}
	}
	if _, err := kube.ContinueToken([]byte(`{"metadata":`)); err == nil {
		t.Error("truncated body decoded")
	}
}

func TestLoadJobEvents(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 1, nil)
	event := func(name, reason, typ string) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": name, "namespace": "team-a"},
			"involvedObject": map[string]any{"kind": "FlinkDeployment", "name": "job-0", "uid": "u1"},
			"reason":         reason, "type": typ, "count": 7}
	}
	f.Put("events", event("e1", "JobException", "Warning"))
	f.Put("events", event("e2", "SpecChanged", "Normal"))
	f.Put("events", event("e3", "JobException", "Normal"))
	c := client(t, f)
	plan := kube.Plan{Read: []string{"team-a"}}
	for _, q := range []kube.Query{{Failures: true}, {Match: &kube.Match{Name: "job-0"}, Failures: true}} {
		s, err := c.Load(context.Background(), plan, q)
		if err != nil || len(s.JobEvents) != 1 || s.JobEvents[0].Name != "e1" || s.JobEvents[0].Times() != 7 {
			t.Errorf("match %v: %v %+v", q.Match != nil, err, s.JobEvents)
		}
	}
	// Without Failures, no events are read.
	if s, _ := c.Load(context.Background(), plan, kube.Query{}); s.JobEvents != nil || s.EventsErr != nil {
		t.Errorf("events read unasked: %+v", s.JobEvents)
	}
	// Events that may not be listed only leave the check out, and say so.
	f.Set(func() { f.Forbidden["events/team-a"] = true })
	if s, err := c.Load(context.Background(), plan, kube.Query{Failures: true}); err != nil || len(s.Deployments) != 1 || s.JobEvents != nil || s.EventsErr["team-a"] == nil {
		t.Errorf("forbidden events: %v %+v", err, s)
	}
}
