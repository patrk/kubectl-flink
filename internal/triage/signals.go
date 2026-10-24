package triage

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
)

// Signal is one fact rules match against: a kind from a fixed vocabulary and its text.
type Signal struct {
	Kind  string `json:"kind"`
	Text  string `json:"text"`
	Layer Layer  `json:"layer"`
	// At is when it happened, for signals that know: events, exceptions, terminations, spec changes.
	At time.Time `json:"at,omitzero"`
}

// adder adds a signal; at is given when the signal knows when it happened.
type adder func(kind string, layer Layer, text string, at ...time.Time)

// Signal kinds. Rules may only name these; the rule loader rejects others.
const (
	SigLifecycle        = "status.lifecycle"    // STABLE, FAILED, ...
	SigJobManager       = "status.jobmanager"   // READY, ERROR, ...
	SigJobState         = "status.job"          // RUNNING, RESTARTING, ...
	SigOperatorError    = "operator.error"      // status.error and each cause in its chain
	SigHealth           = "health.finding"      // the reasons behind the health value
	SigEvent            = "event"               // "Reason: message" of a Warning event
	SigPodWaiting       = "pod.waiting"         // "Reason: message (component)" of a waiting container
	SigPodTerminated    = "pod.terminated"      // "Reason exit N: message (component)" of a last termination
	SigPodUnschedulable = "pod.unschedulable"   // "message (component)" from the scheduler
	SigPodMount         = "pod.mount"           // "FailedMount: message (component)" of a pod that is still starting
	SigLog              = "log.jobmanager"      // a problem line of the JobManager's log, current or previous
	SigException        = "exception"           // first line of an exception in the job's history
	SigExceptionCause   = "exception.cause"     // deepest cause of that exception
	SigCheckpointStale  = "checkpoint.stale"    // computed: last completed checkpoint much older than the interval
	SigCheckpointFail   = "checkpoint.failure"  // failure message of the latest failed checkpoint
	SigRestored         = "checkpoint.restored" // "checkpoint #N" or "savepoint #N" the job restored from
	SigRestarts         = "job.restarts"
	SigJobsMultiple     = "jobs.multiple"
	SigSpecChange       = "spec.change"
	SigSpecWriter       = "spec.writer"
	SigBackpressure     = "performance.backpressure"
	SigBottleneck       = "performance.bottleneck"
	SigSinkIdle         = "performance.sink-idle"
	SigHALeftover       = "ha.leftover"
)

// Kinds is the vocabulary rules may use.
var Kinds = []string{
	SigLifecycle, SigJobManager, SigJobState, SigOperatorError, SigHealth, SigEvent, SigPodWaiting, SigPodTerminated, SigPodMount,
	SigPodUnschedulable, SigLog, SigException, SigExceptionCause, SigCheckpointStale, SigCheckpointFail, SigRestored,
	SigRestarts, SigJobsMultiple, SigSpecChange, SigSpecWriter, SigBackpressure, SigBottleneck, SigSinkIdle, SigHALeftover,
}

// Signals reduces evidence to signals, deduplicated, in a stable order.
func Signals(ev *Evidence) []Signal {
	var out []Signal
	add := func(kind string, layer Layer, text string, at ...time.Time) {
		if text = strings.TrimSpace(text); text != "" {
			s := Signal{Kind: kind, Text: text, Layer: layer}
			if len(at) > 0 {
				s.At = at[0]
			}
			out = append(out, s)
		}
	}
	j := ev.Job
	add(SigLifecycle, Operator, j.Lifecycle)
	add(SigJobManager, Operator, j.JMStatus)
	add(SigJobState, Runtime, j.JobState)
	if e := j.Error; e != nil {
		add(SigOperatorError, Operator, e.Summary(1000))
		for _, c := range e.Causes {
			add(SigOperatorError, Operator, strings.TrimPrefix(c.Type+": "+c.Message, ": "))
		}
	}
	for _, f := range ev.Health.Findings {
		add(SigHealth, Operator, f.Reason)
	}
	for i := range ev.Events {
		e := &ev.Events[i]
		switch {
		case e.Type == "Warning":
			add(SigEvent, Kubernetes, e.Reason+": "+e.Message, e.Last())
		case e.Reason == "SpecChanged":
			// "UPGRADE change(s) detected (Diff: FlinkDeploymentSpec[restartNonce : 1 -> 2]), starting ..."
			if _, diff, ok := strings.Cut(e.Message, "Diff: "); ok {
				diff, _, _ = strings.Cut(diff, "), ")
				add(SigSpecChange, Operator, fmt.Sprintf("%s (%s ago)", diff, duration.HumanDuration(ev.Now.Sub(e.Last()))), e.Last())
			}
		}
	}
	// Who owns restartNonce; when it last changed comes from the SpecChanged events, because a manager's time
	// in managedFields moves with every field it writes.
	for _, w := range j.SpecWriters {
		if slices.Contains(w.Fields, "restartNonce") {
			add(SigSpecWriter, Operator, fmt.Sprintf("restartNonce by %s (generation %d)", w.Manager, j.Generation))
		}
	}
	for i := range ev.Pods {
		podSignals(&ev.Pods[i], ev.Events, add)
	}
	for _, l := range ev.Logs {
		add(SigLog, Kubernetes, l.Text)
	}
	if r := ev.REST; r != nil {
		recent := 0
		for i := range r.Exceptions {
			e := &r.Exceptions[i]
			add(SigException, Runtime, e.Message(), e.Timestamp.Time())
			add(SigExceptionCause, Runtime, e.Cause(), e.Timestamp.Time())
			if ev.Now.Sub(e.Timestamp.Time()) <= restartWindow {
				recent++
			}
		}
		// Only a job that is still going keeps restarting; a failed one stopped for good.
		state := j.JobState
		if r.Job != nil {
			state = r.Job.State
		}
		if recent >= restartLoop && !terminal(state) {
			add(SigRestarts, Runtime, fmt.Sprintf("%d failures in the last %s", recent, duration.HumanDuration(restartWindow)))
		}
		if j.Kind == model.KindDeployment && !j.SessionCluster {
			var active []string
			for _, o := range r.Jobs {
				if !terminal(o.State) {
					active = append(active, o.JID)
				}
			}
			if len(active) > 1 {
				add(SigJobsMultiple, HA, fmt.Sprintf("%d jobs run on the application cluster: %s", len(active), strings.Join(active, ", ")))
			}
		}
		if r.Job != nil {
			performanceSignals(r.Job, ev.Metrics, add)
		}
		if cps := r.Checkpoints; cps != nil {
			if j.JobState == "RUNNING" {
				add(SigCheckpointStale, Runtime, StaleCheckpoint(cps, r.CheckpointConfig, ev.Now))
			}
			if f := cps.Latest.Failed; f != nil {
				add(SigCheckpointFail, Runtime, flink.FirstLine(f.FailureMessage))
			}
			if rs := cps.Latest.Restored; rs != nil {
				kind := "checkpoint"
				if rs.IsSavepoint {
					kind = "savepoint"
				}
				add(SigRestored, Runtime, fmt.Sprintf("%s #%d", kind, rs.ID))
			}
		}
	}
	// Grouped by kind, each kind in collection order (newest first for exceptions and events), each signal once.
	if ev.HA != nil {
		haSignals(ev.HA, j, add)
	}
	slices.SortStableFunc(out, func(a, b Signal) int { return strings.Compare(a.Kind, b.Kind) })
	type key struct{ kind, text string }
	seen := map[key]bool{}
	return slices.DeleteFunc(out, func(s Signal) bool {
		k := key{s.Kind, s.Text}
		dup := seen[k]
		seen[k] = true
		return dup
	})
}

// podSignals ends every pod signal with the pod's component, such as "(taskmanager)", so that a rule can tell a
// crashing JobManager from a crashing TaskManager. Init containers count: they hold the main containers back.
func podSignals(p *kube.Pod, events []kube.Event, add adder) {
	component := " (" + cmp.Or(p.Labels[kube.LabelComponent], "pod") + ")"
	for _, c := range p.Status.Conditions {
		if c.Type == "PodScheduled" && c.Status == "False" {
			add(SigPodUnschedulable, Kubernetes, c.Message+component, c.LastTransitionTime.Time)
		}
	}
	starting := p.Status.Phase == "Pending"
	for _, cs := range append(slices.Clone(p.Status.InitContainerStatuses), p.Status.ContainerStatuses...) {
		if w := cs.State.Waiting; w != nil {
			if w.Reason == "ContainerCreating" || w.Reason == "PodInitializing" {
				starting = true
			} else {
				add(SigPodWaiting, Kubernetes, strings.TrimSuffix(w.Reason+": "+w.Message, ": ")+component)
			}
		}
		for _, t := range []*kube.ContainerTerminated{cs.State.Terminated, cs.LastState.Terminated} {
			if t != nil && (t.Reason != "Completed" || t.ExitCode != 0) {
				add(SigPodTerminated, Kubernetes, strings.TrimSuffix(fmt.Sprintf("%s exit %d: %s", t.Reason, t.ExitCode, t.Message), ": ")+component, t.FinishedAt.Time)
			}
		}
	}
	// A volume that cannot be mounted shows only as events. They count for a pod still starting, and only those
	// about this pod since it was created: the operator's own config ConfigMap is briefly missing on every new
	// deployment, and that event outlives the hiccup by an hour.
	if !starting {
		return
	}
	for i := range events {
		e := &events[i]
		if e.Reason == "FailedMount" && e.InvolvedObject.Kind == "Pod" && e.InvolvedObject.Name == p.Name && !e.Last().Before(p.CreationTimestamp.Time) {
			add(SigPodMount, Kubernetes, "FailedMount: "+e.Message+component, e.Last())
		}
	}
}

// terminal reports whether a Flink job state is final.
func terminal(state string) bool {
	switch state {
	case "FINISHED", "CANCELED", "FAILED", "SUSPENDED":
		return true
	}
	return false
}

// A job with restartLoop or more failures within restartWindow is restarting in a loop.
const (
	restartLoop   = 3
	restartWindow = 15 * time.Minute
)

// staleIntervals is how many intervals the last completed checkpoint may lag before it is called stale.
const staleIntervals = 5

// PeriodicOff is the interval Flink reports when periodic checkpoints are disabled.
const PeriodicOff = math.MaxInt64

// StaleCheckpoint explains when a running job's last completed checkpoint is much older than its interval, or
// none has completed while some failed; empty otherwise. Nothing new is then committed by transactional sinks.
func StaleCheckpoint(cps *flink.Checkpoints, cfg *flink.CheckpointConfig, now time.Time) string {
	if cfg == nil || cfg.Interval <= 0 || cfg.Interval == PeriodicOff {
		return ""
	}
	interval := time.Duration(cfg.Interval) * time.Millisecond
	limit := max(staleIntervals*interval, time.Duration(cfg.Timeout)*time.Millisecond+interval)
	last := cps.Latest.Completed
	if last == nil {
		if cps.Counts.Failed > 0 {
			return fmt.Sprintf("no checkpoint has completed; %d failed (interval %s)", cps.Counts.Failed, Short(interval))
		}
		return ""
	}
	if age := now.Sub(last.LatestAck.Time()); age > limit {
		return fmt.Sprintf("last completed checkpoint is %s old, interval %s", duration.HumanDuration(age), Short(interval))
	}
	return ""
}

// Short formats a configured duration: milliseconds stay exact, longer ones read like ages.
func Short(d time.Duration) string {
	if d < time.Second {
		return d.String()
	}
	return duration.HumanDuration(d)
}
