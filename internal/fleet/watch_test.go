package fleet_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/model"
)

const (
	fdFixture   = "../../testdata/fixtures/operator-1.16.1-flink-2.2/flinkdeployment.json"
	podsFixture = "../../testdata/fixtures/operator-1.16.1-flink-2.2/pods.json"
)

func source(t *testing.T, f *kubetest.API, scope kube.Scope) fleet.Source {
	t.Helper()
	c, err := kube.NewClient(f.Start())
	if err != nil {
		t.Fatal(err)
	}
	return fleet.Source{Scope: scope, Client: c, ContextNamespace: "team-a"}
}

func next(t *testing.T, ch <-chan []fleet.Scope) []fleet.Scope {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(10 * time.Second):
		t.Fatal("no update from watch")
		return nil
	}
}

func TestWatchPublishes(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 3, nil)
	f.Add("pods", podsFixture, "team-a", "pod", 2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	go fleet.Watch(ctx, []fleet.Source{source(t, f, kube.Scope{Context: "dev"})}, "", ch)
	s := next(t, ch)
	if len(s) != 1 || s[0].Err != nil || len(s[0].Jobs) != 3 {
		t.Fatalf("initial: %+v", s)
	}
}

func TestWatchFollowsChanges(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 3, nil)
	f.Add("pods", podsFixture, "team-a", "pod", 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	go fleet.Watch(ctx, []fleet.Source{source(t, f, kube.Scope{Context: "dev"})}, "", ch)
	next(t, ch)

	// A burst of 60 updates is published a few times at most, and the last publish has the final state.
	start := time.Now()
	for i := range 20 {
		f.Add("flinkdeployments", fdFixture, "team-a", "job", 3, func(j int, o map[string]any) {
			if j == 1 {
				status(o)["lifecycleState"] = "FAILED"
				status(o)["error"] = "burst " + string(rune('a'+i))
			}
		})
	}
	var last []fleet.Scope
	publishes := 0
	for quiet := false; !quiet; {
		select {
		case s := <-ch:
			last = s
			publishes++
		case <-time.After(3 * fleet.Coalesce):
			quiet = last != nil
		}
	}
	if publishes > 3 {
		t.Errorf("%d publishes for one burst", publishes)
	}
	if time.Since(start) < fleet.Coalesce {
		t.Errorf("published after %s, before coalescing", time.Since(start))
	}
	var failed *model.Job
	for i := range last[0].Jobs {
		if last[0].Jobs[i].Name == "job-1" {
			failed = &last[0].Jobs[i]
		}
	}
	if failed == nil || failed.Lifecycle != "FAILED" || failed.Error == nil || failed.Error.Raw != "burst t" {
		t.Fatalf("final state not seen: %+v", failed)
	}
}

func TestWatchForbiddenNamespaceFailsScope(t *testing.T) {
	f := kubetest.New(t)
	f.Forbidden["flinksessionjobs/team-b"] = true
	ok := kubetest.New(t)
	ok.Add("flinkdeployments", fdFixture, "team-a", "job", 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	go fleet.Watch(ctx, []fleet.Source{
		source(t, f, kube.Scope{Context: "prd", Namespaces: []string{"team-a", "team-b"}}),
		source(t, ok, kube.Scope{Context: "dev"}),
	}, "", ch)
	s := next(t, ch)
	if s[0].Err == nil || !strings.Contains(s[0].Err.Error(), "team-b") || s[1].Err != nil || len(s[1].Jobs) != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestWatchWithoutPods(t *testing.T) {
	f := kubetest.New(t)
	f.Add("flinkdeployments", fdFixture, "team-a", "job", 1, nil)
	f.Forbidden["pods/team-a"] = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	go fleet.Watch(ctx, []fleet.Source{source(t, f, kube.Scope{Context: "dev"})}, "", ch)
	s := next(t, ch)
	if s[0].Err != nil || strings.Join(s[0].PodsUnread, ",") != "team-a" || s[0].Jobs[0].Pods != nil {
		t.Errorf("%+v", s[0])
	}
}

func TestWatchUnresolvableScope(t *testing.T) {
	f := kubetest.New(t)
	f.NotFound = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	go fleet.Watch(ctx, []fleet.Source{
		source(t, f, kube.Scope{Context: "dev", AllNamespaces: true}),
		{Scope: kube.Scope{Context: "gone"}, Err: errors.New("no such context")},
	}, "", ch)
	s := next(t, ch)
	if !errors.Is(s[0].Err, kube.ErrNoOperator) || s[1].Err == nil {
		t.Errorf("%v / %v", s[0].Err, s[1].Err)
	}
}

func TestWatchDoesNotWaitForBrokenScope(t *testing.T) {
	defer func(d time.Duration) { fleet.SyncTimeout = d }(fleet.SyncTimeout)
	fleet.SyncTimeout = time.Second
	broken := kubetest.New(t)
	broken.BrokenInformers = true
	ok := kubetest.New(t)
	ok.Add("flinkdeployments", fdFixture, "team-a", "job", 2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []fleet.Scope)
	start := time.Now()
	go fleet.Watch(ctx, []fleet.Source{
		source(t, broken, kube.Scope{Context: "prd"}), source(t, ok, kube.Scope{Context: "dev"}),
	}, "", ch)
	s := next(t, ch)
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("first publish after %s", took)
	}
	if s[0].Err != nil || s[0].WatchErr == nil || !strings.Contains(s[0].WatchErr.Error(), "not in sync") {
		t.Errorf("broken scope: err %v, watch err %v", s[0].Err, s[0].WatchErr)
	}
	if s[1].Err != nil || s[1].WatchErr != nil || len(s[1].Jobs) != 2 {
		t.Errorf("healthy scope: %+v", s[1])
	}
}
