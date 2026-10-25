package kube

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Snapshot is everything a one-shot command reads from one context.
type Snapshot struct {
	Plan        Plan
	Deployments []FlinkDeployment
	SessionJobs []FlinkSessionJob
	Pods        []Pod
	// Snapshots are the completed FlinkStateSnapshots, for the age of each job's newest; empty when the CRD is
	// missing or they may not be listed, which only blanks that column.
	Snapshots []FlinkStateSnapshot
	// PodsErr holds, per namespace read ("" for cluster scope), why its pods could not be read. Pods only add
	// the TaskManager check, so this never fails a load; jobs in those namespaces get no pod data.
	PodsErr map[string]error
}

// PodsRead reports whether the pods of namespace were read.
func (s *Snapshot) PodsRead(namespace string) bool {
	if _, ok := s.PodsErr[""]; ok {
		return false
	}
	_, failed := s.PodsErr[namespace]
	return !failed
}

// parallelLists bounds concurrent LIST requests per context.
const parallelLists = 8

// Query is what a load reads beyond deployments, session jobs and pods.
type Query struct {
	// Selector filters the deployments and session jobs on the server.
	Selector string
	// Snapshots also reads completed FlinkStateSnapshots, for the snapshot age.
	Snapshots bool
	// Match, when set, reads only the jobs it names, filtered by the API server.
	Match *Match
}

// Match names jobs the API server can find without the whole fleet being read: by exact name, or by a value of
// one of the given label keys. Pods and snapshots are then read only in the namespaces of the jobs found.
type Match struct {
	// Name is an exact resource name; empty, or not a valid name, reads no job by name.
	Name string
	// Labels are label keys whose value must be Value; a Value that cannot be a label value reads none.
	Labels []string
	Value  string
}

// selectors are the label and field selectors of the lists a match needs, each ANDed with the query's selector.
func (m *Match) selectors(base string) (labels, fields []string) {
	and := func(sel string) string {
		if base == "" {
			return sel
		}
		return base + "," + sel
	}
	if m.Name != "" && len(validation.IsDNS1123Subdomain(m.Name)) == 0 {
		labels, fields = append(labels, base), append(fields, "metadata.name="+m.Name)
	}
	if len(validation.IsValidLabelValue(m.Value)) == 0 {
		for _, k := range m.Labels {
			labels, fields = append(labels, and(k+"="+m.Value)), append(fields, "")
		}
	}
	return labels, fields
}

// Load lists deployments, session jobs and Flink pods in every namespace of p, in parallel.
func (c *Client) Load(ctx context.Context, p Plan, q Query) (*Snapshot, error) {
	if q.Match != nil {
		return c.loadMatch(ctx, p, q)
	}
	type part struct {
		fds  *FlinkDeploymentList
		sjs  *FlinkSessionJobList
		pods *PodList
		perr error
		snap *FlinkStateSnapshotList
	}
	parts := make([]part, len(p.Read))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelLists)
	for i, ns := range p.Read {
		g.Go(func() error {
			obj, err := c.List(ctx, Deployments, ns, q.Selector)
			if err != nil {
				return ListError(p, ns, err)
			}
			parts[i].fds = obj.(*FlinkDeploymentList)
			return nil
		})
		g.Go(func() error {
			obj, err := c.List(ctx, SessionJobs, ns, q.Selector)
			if err != nil {
				return ListError(p, ns, err)
			}
			parts[i].sjs = obj.(*FlinkSessionJobList)
			return nil
		})
		g.Go(func() error {
			// A pods error is kept, not returned: it must not cancel the other lists.
			obj, err := c.List(ctx, Pods, ns, FlinkPodSelector)
			if err == nil {
				parts[i].pods = obj.(*PodList)
			}
			parts[i].perr = err
			return nil
		})
		if !q.Snapshots {
			continue
		}
		g.Go(func() error {
			// Only completed ones, by the label the operator sets; an error only leaves the column empty.
			if obj, err := c.List(ctx, Snapshots, ns, LabelSnapshotState+"=COMPLETED"); err == nil {
				parts[i].snap = obj.(*FlinkStateSnapshotList)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	s := &Snapshot{Plan: p}
	for i, pt := range parts {
		s.Deployments = append(s.Deployments, pt.fds.Items...)
		s.SessionJobs = append(s.SessionJobs, pt.sjs.Items...)
		if pt.snap != nil {
			s.Snapshots = append(s.Snapshots, pt.snap.Items...)
		}
		if pt.perr != nil {
			if s.PodsErr == nil {
				s.PodsErr = map[string]error{}
			}
			s.PodsErr[p.Read[i]] = pt.perr
			continue
		}
		s.Pods = append(s.Pods, pt.pods.Items...)
	}
	return s, nil
}

// ListError says where a list failed, and turns NotFound into ErrNoOperator.
func ListError(p Plan, ns string, err error) error {
	where := "context " + p.Context
	if ns != "" {
		where += ", namespace " + ns
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%s: %w", where, ErrNoOperator)
	}
	return fmt.Errorf("%s: %w", where, err)
}

// matchNamespaces is how many namespaces a match lists one by one; past it, its lists outnumber a full load's,
// and the full load is matched instead.
const matchNamespaces = parallelLists

// matches reports whether the API server would find m by the match's name or labels.
func (m *Match) matches(o *ObjectMeta) bool {
	if m.Name != "" && o.Name == m.Name {
		return true
	}
	return slices.ContainsFunc(m.Labels, func(k string) bool { v, ok := o.Labels[k]; return ok && v == m.Value })
}

// loadMatch lists the jobs q.Match names, then the pods and snapshots of their namespaces only.
func (c *Client) loadMatch(ctx context.Context, p Plan, q Query) (*Snapshot, error) {
	if len(p.Read) > matchNamespaces {
		full := q
		full.Match = nil
		s, err := c.Load(ctx, p, full)
		if err != nil {
			return nil, err
		}
		s.Deployments = slices.DeleteFunc(s.Deployments, func(d FlinkDeployment) bool { return !q.Match.matches(&d.ObjectMeta) })
		s.SessionJobs = slices.DeleteFunc(s.SessionJobs, func(j FlinkSessionJob) bool { return !q.Match.matches(&j.ObjectMeta) })
		return s, nil
	}
	labels, fields := q.Match.selectors(q.Selector)
	var mu sync.Mutex
	s := &Snapshot{Plan: p}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelLists)
	for _, ns := range p.Read {
		for i := range labels {
			g.Go(func() error {
				return listInto(gctx, c, p, ns, Deployments, labels[i], fields[i], &mu, &s.Deployments,
					func(l *FlinkDeploymentList) []FlinkDeployment { return l.Items })
			})
			g.Go(func() error {
				return listInto(gctx, c, p, ns, SessionJobs, labels[i], fields[i], &mu, &s.SessionJobs,
					func(l *FlinkSessionJobList) []FlinkSessionJob { return l.Items })
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	// A job two selectors find is kept once; the order is the full load's, so that output is stable.
	s.Deployments = sortedUnique(s.Deployments, func(d *FlinkDeployment) *ObjectMeta { return &d.ObjectMeta })
	s.SessionJobs = sortedUnique(s.SessionJobs, func(j *FlinkSessionJob) *ObjectMeta { return &j.ObjectMeta })

	// The clusters found, by namespace: a session job runs on its session cluster.
	clusters := map[string][]string{}
	for i := range s.Deployments {
		clusters[s.Deployments[i].Namespace] = append(clusters[s.Deployments[i].Namespace], s.Deployments[i].Name)
	}
	for i := range s.SessionJobs {
		sj := &s.SessionJobs[i]
		clusters[sj.Namespace] = append(clusters[sj.Namespace], sj.Spec.DeploymentName)
	}
	g, gctx = errgroup.WithContext(ctx)
	g.SetLimit(parallelLists)
	for ns, names := range clusters {
		g.Go(func() error {
			obj, err := c.List(gctx, Pods, ns, clusterPods(names))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if s.PodsErr == nil {
					s.PodsErr = map[string]error{}
				}
				s.PodsErr[ns] = err
				return nil
			}
			s.Pods = append(s.Pods, obj.(*PodList).Items...)
			return nil
		})
		if q.Snapshots {
			g.Go(func() error {
				if obj, err := c.List(gctx, Snapshots, ns, LabelSnapshotState+"=COMPLETED"); err == nil {
					mu.Lock()
					defer mu.Unlock()
					s.Snapshots = append(s.Snapshots, obj.(*FlinkStateSnapshotList).Items...)
				}
				return nil
			})
		}
	}
	return s, g.Wait()
}

// clusterPods selects the Flink pods of the named clusters, or every Flink pod of the namespace when a name
// cannot be a label value, such as one longer than 63 characters.
func clusterPods(names []string) string {
	slices.Sort(names)
	names = slices.Compact(names)
	for _, n := range names {
		if n == "" || len(validation.IsValidLabelValue(n)) > 0 {
			return FlinkPodSelector
		}
	}
	return FlinkPodSelector + "," + LabelApp + " in (" + strings.Join(names, ",") + ")"
}

// listInto lists r with the selectors and appends the items to dst.
func listInto[L any, T any](ctx context.Context, c *Client, p Plan, ns string, r Resource, labels, fields string,
	mu *sync.Mutex, dst *[]T, items func(*L) []T) error {
	obj, err := c.ListMatching(ctx, r, ns, labels, fields)
	if err != nil {
		return ListError(p, ns, err)
	}
	mu.Lock()
	defer mu.Unlock()
	*dst = append(*dst, items(any(obj).(*L))...)
	return nil
}

// sortedUnique orders items by namespace and name and drops repeats.
func sortedUnique[T any](items []T, meta func(*T) *ObjectMeta) []T {
	key := func(t *T) string { m := meta(t); return m.Namespace + "/" + m.Name }
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(key(&a), key(&b)) })
	return slices.CompactFunc(items, func(a, b T) bool { return key(&a) == key(&b) })
}
