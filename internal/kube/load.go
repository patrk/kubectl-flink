package kube

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
}

// Load lists deployments, session jobs and Flink pods in every namespace of p, in parallel.
func (c *Client) Load(ctx context.Context, p Plan, q Query) (*Snapshot, error) {
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
