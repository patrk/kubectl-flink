//go:build e2e

package kube

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/tools/cache"
)

// recorder records the query of every request, to tell WatchList from LIST on a real API server.
type recorder struct {
	mu      sync.Mutex
	queries []string
	next    http.RoundTripper
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.queries = append(r.queries, req.URL.Path+"?"+req.URL.RawQuery)
	r.mu.Unlock()
	return r.next.RoundTrip(req)
}

func (r *recorder) count(sub ...string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, q := range r.queries {
		all := true
		for _, s := range sub {
			all = all && strings.Contains(q, s)
		}
		if all {
			n++
		}
	}
	return n
}

// TestLive runs against the e2e kind cluster with the operator and the "basic" job (make e2e).
func TestLive(t *testing.T) {
	kctx := os.Getenv("KUBE_CONTEXT")
	if kctx == "" {
		t.Skip("KUBE_CONTEXT not set")
	}
	flags := genericclioptions.NewConfigFlags(false)
	flags.Context = &kctx
	cs, err := NewContexts(flags)
	if err != nil {
		t.Fatal(err)
	}
	cfg, ns, err := cs.Config("")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper { rec.next = rt; return rec })
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	plan, err := c.Resolve(ctx, Scope{Context: kctx, AllNamespaces: true}, ns)
	if err != nil || len(plan.Read) != 1 || plan.Read[0] != "" {
		t.Fatalf("cluster scope: %+v %v", plan, err)
	}
	snap, err := c.Load(ctx, plan, Query{})
	if err != nil {
		t.Fatal(err)
	}
	var basic *FlinkDeployment
	for i := range snap.Deployments {
		if snap.Deployments[i].Name == "basic" {
			basic = &snap.Deployments[i]
		}
	}
	if basic == nil || basic.Status.LifecycleState == "" || len(snap.Pods) < 2 || len(snap.PodsErr) > 0 {
		t.Fatalf("load: basic %v, %d pods, pods error %v", basic != nil, len(snap.Pods), snap.PodsErr)
	}

	for _, r := range []Resource{Deployments, Pods} {
		sel := ""
		if r.GVR.Resource == "pods" {
			sel = FlinkPodSelector
		}
		inf := c.Informer(r, "", sel, nil)
		ictx, icancel := context.WithCancel(ctx)
		go inf.RunWithContext(ictx)
		if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
			t.Fatalf("%s informer did not sync", r.GVR.Resource)
		}
		if len(inf.GetStore().List()) == 0 {
			t.Errorf("%s informer is empty", r.GVR.Resource)
		}
		icancel()
	}
	if n := rec.count("sendInitialEvents=true"); n < 2 {
		t.Errorf("%d WatchList requests, want one per informer: %v", n, rec.queries)
	}
	// Load and Resolve LIST once per resource; informers must not add any.
	if n := rec.count("/flinkdeployments?", "limit="); n != 2 {
		t.Errorf("%d LIST requests for flinkdeployments, want 2 (probe and load): %v", n, rec.queries)
	}
}
