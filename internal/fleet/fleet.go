// Package fleet loads every scope into jobs and keeps them current. It is the one collector behind ls and top:
// a one-shot load and a watch produce the same jobs through the same conversion.
package fleet

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
)

// Scope is what one scope yielded.
type Scope struct {
	kube.Scope
	// Note explains a fallback, such as cluster scope being forbidden.
	Note string
	// Err is set when the scope could not be read; it is shown as an error row, the other scopes still load.
	Err error
	// WatchErr is set while a watched scope's informers fail or have not synced; its jobs may be stale.
	WatchErr error
	// PodsUnread lists the namespaces whose pods could not be read ("" for cluster scope); jobs there have no
	// pod data.
	PodsUnread []string
	Jobs       []model.Job
}

// Source is a scope with its client.
type Source struct {
	Scope            kube.Scope
	Client           *kube.Client
	ContextNamespace string
	// Err is set when no client could be built, such as for a context missing from the kubeconfig.
	Err error
}

// Sources builds a client per scope.
func Sources(cs *kube.Contexts, scopes []kube.Scope) []Source {
	out := make([]Source, len(scopes))
	for i, s := range scopes {
		out[i].Scope = s
		cfg, ns, err := cs.Config(s.Context)
		if err == nil {
			out[i].Client, err = kube.NewClient(cfg)
		}
		out[i].ContextNamespace, out[i].Err = ns, err
	}
	return out
}

// Load reads all sources in parallel. labelSelector filters deployments and session jobs on the server.
func Load(ctx context.Context, sources []Source, labelSelector string) []Scope {
	out := make([]Scope, len(sources))
	var wg sync.WaitGroup
	for i, src := range sources {
		wg.Go(func() { out[i] = load(ctx, src, labelSelector) })
	}
	wg.Wait()
	return out
}

func load(ctx context.Context, src Source, labelSelector string) Scope {
	s := Scope{Scope: src.Scope, Err: src.Err}
	if s.Err != nil {
		return s
	}
	plan, err := src.Client.Resolve(ctx, src.Scope, src.ContextNamespace)
	s.Note = plan.Note
	if err != nil {
		s.Err = err
		return s
	}
	snap, err := src.Client.Load(ctx, plan, labelSelector)
	if err != nil {
		s.Err = err
		return s
	}
	s.Jobs, s.PodsUnread = Jobs(src.Scope.Context, snap)
	return s
}

// Jobs converts a snapshot into jobs with their pods attached, and lists the namespaces whose pods were not read.
func Jobs(kubeContext string, snap *kube.Snapshot) (jobs []model.Job, podsUnread []string) {
	jobs = make([]model.Job, 0, len(snap.Deployments)+len(snap.SessionJobs))
	for i := range snap.Deployments {
		jobs = append(jobs, model.FromDeployment(kubeContext, &snap.Deployments[i]))
	}
	for i := range snap.SessionJobs {
		jobs = append(jobs, model.FromSessionJob(kubeContext, &snap.SessionJobs[i]))
	}
	model.AttachPods(jobs, model.SummarisePods(snap.Pods), snap.PodsRead)
	unread := make([]string, 0, len(snap.PodsErr))
	for ns := range snap.PodsErr {
		unread = append(unread, ns)
	}
	slices.Sort(unread)
	return jobs, unread
}

var errPodsUnread = errors.New("pods could not be read")
