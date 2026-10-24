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
)

// Signal is one fact rules match against: a kind from a fixed vocabulary and its text.
type Signal struct {
	Kind  string `json:"kind"`
	Text  string `json:"text"`
	Layer Layer  `json:"layer"`
}

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
	SigLog              = "log.jobmanager"      // a problem line of the JobManager's log, current or previous
	SigException        = "exception"           // first line of an exception in the job's history
	SigExceptionCause   = "exception.cause"     // deepest cause of that exception
	SigCheckpointStale  = "checkpoint.stale"    // computed: last completed checkpoint much older than the interval
	SigCheckpointFail   = "checkpoint.failure"  // failure message of the latest failed checkpoint
	SigRestored         = "checkpoint.restored" // "checkpoint #N" or "savepoint #N" the job restored from
)

// Kinds is the vocabulary rules may use.
var Kinds = []string{
	SigLifecycle, SigJobManager, SigJobState, SigOperatorError, SigHealth, SigEvent, SigPodWaiting, SigPodTerminated,
	SigPodUnschedulable, SigLog, SigException, SigExceptionCause, SigCheckpointStale, SigCheckpointFail, SigRestored,
}

// Signals reduces evidence to signals, deduplicated, in a stable order.
func Signals(ev *Evidence) []Signal {
	var out []Signal
	add := func(kind string, layer Layer, text string) {
		if text = strings.TrimSpace(text); text != "" {
			out = append(out, Signal{Kind: kind, Text: text, Layer: layer})
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
		if e := &ev.Events[i]; e.Type == "Warning" {
			add(SigEvent, Kubernetes, e.Reason+": "+e.Message)
		}
	}
	for i := range ev.Pods {
		podSignals(&ev.Pods[i], add)
	}
	for _, l := range ev.Logs {
		add(SigLog, Kubernetes, l.Text)
	}
	if r := ev.REST; r != nil {
		for i := range r.Exceptions {
			e := &r.Exceptions[i]
			add(SigException, Runtime, e.Message())
			add(SigExceptionCause, Runtime, e.Cause())
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
	slices.SortStableFunc(out, func(a, b Signal) int { return strings.Compare(a.Kind, b.Kind) })
	seen := map[Signal]bool{}
	return slices.DeleteFunc(out, func(s Signal) bool {
		dup := seen[s]
		seen[s] = true
		return dup
	})
}

// podSignals ends every pod signal with the pod's component, such as "(taskmanager)", so that a rule can tell a
// crashing JobManager from a crashing TaskManager.
func podSignals(p *kube.Pod, add func(kind string, layer Layer, text string)) {
	component := " (" + cmp.Or(p.Labels[kube.LabelComponent], "pod") + ")"
	for _, c := range p.Status.Conditions {
		if c.Type == "PodScheduled" && c.Status == "False" {
			add(SigPodUnschedulable, Kubernetes, c.Message+component)
		}
	}
	for _, cs := range p.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil && w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" {
			add(SigPodWaiting, Kubernetes, strings.TrimSuffix(w.Reason+": "+w.Message, ": ")+component)
		}
		for _, t := range []*kube.ContainerTerminated{cs.State.Terminated, cs.LastState.Terminated} {
			if t != nil && (t.Reason != "Completed" || t.ExitCode != 0) {
				add(SigPodTerminated, Kubernetes, strings.TrimSuffix(fmt.Sprintf("%s exit %d: %s", t.Reason, t.ExitCode, t.Message), ": ")+component)
			}
		}
	}
}

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
