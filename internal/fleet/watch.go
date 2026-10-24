package fleet

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"k8s.io/client-go/tools/cache"

	"github.com/patrk/kubectl-flink/internal/kube"
)

// SyncTimeout bounds how long Watch waits for one scope's caches before it publishes without them, so that a
// scope whose API server keeps failing does not hold back the others.
var SyncTimeout = 30 * time.Second

// Coalesce is how long the watch gathers informer events before it publishes, so a burst of updates costs one
// re-derivation and one redraw.
const Coalesce = 250 * time.Millisecond

// watched is one scope's informers.
type watched struct {
	src     Source
	plan    kube.Plan
	fds     []cache.SharedIndexInformer
	sjs     []cache.SharedIndexInformer
	pods    []cache.SharedIndexInformer
	unread  []string
	mu      sync.Mutex
	err     error // the last watch error, cleared by the next event
	initErr error // the scope could not be resolved; no informers run
}

// Watch keeps every source current with informers and sends the scopes after the initial sync and then after
// every burst of changes, at most once per Coalesce. It returns when ctx ends. Health changes that come from
// time passing alone have no event; the caller re-assesses on its own tick.
func Watch(ctx context.Context, sources []Source, labelSelector string, out chan<- []Scope) {
	dirty := make(chan struct{}, 1)
	mark := func() {
		select {
		case dirty <- struct{}{}:
		default:
		}
	}
	ws := make([]*watched, len(sources))
	var wg sync.WaitGroup
	for i, src := range sources {
		ws[i] = &watched{src: src}
		wg.Go(func() { ws[i].start(ctx, labelSelector, mark) })
	}
	wg.Wait()

	publish := func() {
		select {
		case out <- snapshot(ws):
		case <-ctx.Done():
		}
	}
	publish()
	for {
		select {
		case <-ctx.Done():
			return
		case <-dirty:
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(Coalesce):
		}
		// Events that arrived during the pause are covered by this publish.
		select {
		case <-dirty:
		default:
		}
		publish()
	}
}

// start resolves the scope, starts its informers and waits for them to sync, at most SyncTimeout.
func (w *watched) start(ctx context.Context, labelSelector string, mark func()) {
	if w.initErr = w.src.Err; w.initErr != nil {
		return
	}
	c := w.src.Client
	if w.plan, w.initErr = c.Resolve(ctx, w.src.Scope, w.src.ContextNamespace); w.initErr != nil {
		return
	}
	podsOK, err := w.probe(ctx)
	if err != nil {
		w.initErr = err
		return
	}
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { w.clearErr(); mark() },
		UpdateFunc: func(any, any) { w.clearErr(); mark() },
		DeleteFunc: func(any) { w.clearErr(); mark() },
	}
	onErr := func(_ context.Context, _ *cache.Reflector, err error) {
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		mark()
	}
	var synced []cache.InformerSynced
	run := func(inf cache.SharedIndexInformer) {
		_ = inf.SetWatchErrorHandlerWithContext(onErr) // only fails once the informer runs
		_, _ = inf.AddEventHandler(handler)
		go inf.RunWithContext(ctx)
		synced = append(synced, inf.HasSynced)
	}
	for i, ns := range w.plan.Read {
		fd := c.Informer(kube.Deployments, ns, labelSelector, nil)
		sj := c.Informer(kube.SessionJobs, ns, labelSelector, nil)
		run(fd)
		run(sj)
		w.fds, w.sjs = append(w.fds, fd), append(w.sjs, sj)
		// Pods only add the TaskManager check: where they are forbidden, the scope goes on without them.
		if !podsOK[i] {
			w.unread = append(w.unread, ns)
			continue
		}
		pods := c.Informer(kube.Pods, ns, kube.FlinkPodSelector, nil)
		run(pods)
		w.pods = append(w.pods, pods)
	}
	sctx, cancel := context.WithTimeout(ctx, SyncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(sctx.Done(), synced...) && ctx.Err() == nil {
		w.mu.Lock()
		w.err = fmt.Errorf("not in sync after %s, showing what has arrived: %w", SyncTimeout, cmp.Or(w.err, errNoSync))
		w.mu.Unlock()
	}
}

var errNoSync = errors.New("no response")

// probe checks every list before any informer starts, in parallel, because a forbidden informer never syncs.
// A forbidden deployment or session job list fails the scope, as it does in Load; a forbidden pod list only
// drops pods. At cluster scope Resolve has already probed the Flink lists.
func (w *watched) probe(ctx context.Context) (podsOK []bool, err error) {
	c := w.src.Client
	podsOK = make([]bool, len(w.plan.Read))
	clusterProbed := w.plan.AllNamespaces && len(w.plan.Read) == 1 && w.plan.Read[0] == ""
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, ns := range w.plan.Read {
		if !clusterProbed {
			for _, r := range []kube.Resource{kube.Deployments, kube.SessionJobs} {
				g.Go(func() error {
					if err := c.Probe(gctx, r, ns); err != nil {
						return kube.ListError(w.plan, ns, err)
					}
					return nil
				})
			}
		}
		g.Go(func() error {
			podsOK[i] = c.Probe(gctx, kube.Pods, ns) == nil
			return nil
		})
	}
	return podsOK, g.Wait()
}

func (w *watched) clearErr() {
	w.mu.Lock()
	w.err = nil
	w.mu.Unlock()
}

// snapshot builds the scopes from the informer caches through the same conversion as Load.
func snapshot(ws []*watched) []Scope {
	out := make([]Scope, len(ws))
	for i, w := range ws {
		s := Scope{Scope: w.src.Scope, Note: w.plan.Note, Err: w.initErr}
		w.mu.Lock()
		s.WatchErr = w.err
		w.mu.Unlock()
		if w.initErr == nil {
			snap := &kube.Snapshot{Plan: w.plan}
			for _, inf := range w.fds {
				for _, o := range inf.GetStore().List() {
					snap.Deployments = append(snap.Deployments, *o.(*kube.FlinkDeployment))
				}
			}
			for _, inf := range w.sjs {
				for _, o := range inf.GetStore().List() {
					snap.SessionJobs = append(snap.SessionJobs, *o.(*kube.FlinkSessionJob))
				}
			}
			for _, inf := range w.pods {
				for _, o := range inf.GetStore().List() {
					snap.Pods = append(snap.Pods, *o.(*kube.Pod))
				}
			}
			if len(w.unread) > 0 {
				snap.PodsErr = map[string]error{}
				for _, ns := range w.unread {
					snap.PodsErr[ns] = errPodsUnread
				}
			}
			s.Jobs, s.PodsUnread = Jobs(w.src.Scope.Context, snap)
		}
		out[i] = s
	}
	return out
}
