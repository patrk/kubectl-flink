package triage

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
)

// JobSnapshots lists the job's FlinkStateSnapshots, newest first. Processed ones are found by the label the operator
// sets; ones it never processed carry no labels and are found among the unlabelled, by spec.jobReference.
func JobSnapshots(ctx context.Context, kc *kube.Client, job *model.Job) ([]kube.FlinkStateSnapshot, error) {
	var out []kube.FlinkStateSnapshot
	for _, selector := range []string{kube.LabelSnapshotJob + "=" + job.Name, "!" + kube.LabelSnapshotState} {
		obj, err := kc.List(ctx, kube.Snapshots, job.Namespace, selector)
		if err != nil {
			return nil, err
		}
		items := obj.(*kube.FlinkStateSnapshotList).Items
		for i := range items {
			ref := items[i].Spec.JobReference
			dup := slices.ContainsFunc(out, func(s kube.FlinkStateSnapshot) bool { return s.UID == items[i].UID })
			if ref.Name == job.Name && (ref.Kind == "" || ref.Kind == string(job.Kind)) && !dup {
				out = append(out, items[i])
			}
		}
	}
	slices.SortStableFunc(out, func(a, b kube.FlinkStateSnapshot) int {
		return b.CreationTimestamp.Compare(a.CreationTimestamp.Time)
	})
	return out, nil
}

// IgnoredAfter is how long a snapshot may go without a status before the operator is taken not to process it.
const IgnoredAfter = 2 * time.Minute

// snapshotSignals reports snapshots the operator ignores, the newest completed one of each type, and failed ones
// no newer snapshot of their type has completed since. snaps are newest first.
func snapshotSignals(snaps []kube.FlinkStateSnapshot, now time.Time, add adder) {
	ignored := 0
	var oldest time.Time
	completed := map[string]bool{}
	for i := range snaps {
		s := &snaps[i]
		created := s.CreationTimestamp.Time
		switch st := s.Status.State; {
		case st == "" && now.Sub(created) > IgnoredAfter:
			ignored++
			if oldest.IsZero() || created.Before(oldest) {
				oldest = created
			}
		case st == "COMPLETED":
			if completed[s.Type()] {
				continue
			}
			completed[s.Type()] = true
			at := cmpTime(s.Result(), created)
			add(SigSnapshotDone, Runtime, fmt.Sprintf("%s %s %s ago: %s", s.Type(), s.Name, duration.HumanDuration(now.Sub(at)), s.Status.Path), at)
		case completed[s.Type()]:
			// A newer snapshot of this type completed: the failure is history.
		case st == "FAILED" || st == "ABANDONED" || s.Status.Failures > 0:
			text := strings.TrimSpace(s.Type() + " " + s.Name + " " + st)
			if s.Status.Failures > 0 {
				text += fmt.Sprintf(" after %d failures", s.Status.Failures)
			}
			if e := model.ParseStatusError(s.Status.Error); e != nil {
				text += ": " + e.Summary(300)
			}
			add(SigSnapshotFail, Runtime, text, cmpTime(s.Result(), created))
		}
	}
	if ignored > 0 {
		add(SigSnapshotIgnored, Operator, fmt.Sprintf("%d FlinkStateSnapshots without a status, the oldest created %s ago",
			ignored, duration.HumanDuration(now.Sub(oldest))), oldest)
	}
}

func cmpTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	return a
}
