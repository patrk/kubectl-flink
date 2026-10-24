package model

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"
)

// Health is the single derived state used by ls, filters, sorting, why and the TUI.
type Health int

// Health values, ordered from healthy to worst so that a higher value sorts first when triaging.
const (
	OK Health = iota
	Suspended
	Progressing
	Degraded
	Stuck
	Failed
)

var healthNames = [...]string{"OK", "SUSPENDED", "PROGRESSING", "DEGRADED", "STUCK", "FAILED"}

func (h Health) String() string {
	if int(h) < len(healthNames) {
		return healthNames[h]
	}
	return fmt.Sprintf("Health(%d)", int(h))
}

// ParseHealth parses a health name, case-insensitively.
func ParseHealth(s string) (Health, error) {
	for i, n := range healthNames {
		if strings.EqualFold(n, s) {
			return Health(i), nil
		}
	}
	return 0, fmt.Errorf("unknown health %q", s)
}

// MarshalText implements encoding.TextMarshaler, so JSON and YAML output use the name.
func (h Health) MarshalText() ([]byte, error) { return []byte(h.String()), nil }

// Thresholds turn transient states into DEGRADED or STUCK once they last too long.
type Thresholds struct {
	// Grace covers startup noise, such as a JobManager ERROR while the config ConfigMap is still being created.
	Grace time.Duration
	// DegradedAfter applies to a job that is restarting, reconciling or not yet ready.
	DegradedAfter time.Duration
	// StuckAfter applies to upgrades, rollbacks, deployments and suspends that do not finish.
	StuckAfter time.Duration
	// DriftAfter applies to a spec change the operator has not observed.
	DriftAfter time.Duration
}

// ThresholdsFor derives thresholds from the operator's reconcile interval, so that a job is never called
// unhealthy before the operator has had several chances to act. The floors match the operator's own 5 minute
// readiness timeout.
func ThresholdsFor(reconcileInterval time.Duration) Thresholds {
	ri := reconcileInterval
	return Thresholds{
		Grace:         2 * ri,
		DegradedAfter: max(5*time.Minute, 5*ri),
		StuckAfter:    max(10*time.Minute, 10*ri),
		DriftAfter:    max(5*time.Minute, 5*ri),
	}
}

// DefaultThresholds are ThresholdsFor the operator's default reconcile interval of 60 s.
func DefaultThresholds() Thresholds { return ThresholdsFor(time.Minute) }

// Finding is one reason contributing to a job's health.
type Finding struct {
	Health Health `json:"health"`
	Reason string `json:"reason"`
	// Since is when a state that has lasted too long began. It is kept apart from Reason so that the reason
	// stays the same while time passes; Text adds the duration.
	Since *time.Time `json:"since,omitempty"`
}

// Text is the reason with how long the state has lasted at now, such as "job RESTARTING for 20m".
func (f *Finding) Text(now time.Time) string {
	if f.Since == nil {
		return f.Reason
	}
	return f.Reason + " for " + duration.HumanDuration(now.Sub(*f.Since))
}

// Assessment is a job's health and the findings behind it, worst first.
type Assessment struct {
	Health   Health    `json:"health"`
	Findings []Finding `json:"findings,omitempty"`
}

// Assess derives the health of j at time now.
func Assess(j *Job, now time.Time, th Thresholds) Assessment {
	a := &assessor{j: j, now: now, th: th}
	a.run()
	sort.SliceStable(a.findings, func(x, y int) bool { return a.findings[x].Health > a.findings[y].Health })
	h := OK
	if len(a.findings) > 0 {
		h = a.findings[0].Health
	}
	return Assessment{Health: h, Findings: a.findings}
}

type assessor struct {
	j        *Job
	now      time.Time
	th       Thresholds
	findings []Finding
}

func (a *assessor) add(h Health, format string, args ...any) {
	a.findings = append(a.findings, Finding{Health: h, Reason: fmt.Sprintf(format, args...)})
}

// first returns the first non-zero time; zero if none is known.
func first(ts ...time.Time) time.Time {
	for _, t := range ts {
		if !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

// timed adds a finding that is PROGRESSING until the state that began at start has lasted longer than limit,
// then late. A zero start is unknown; the finding stays PROGRESSING because there is no evidence that it has
// lasted too long.
func (a *assessor) timed(start time.Time, limit time.Duration, late Health, what string) {
	switch {
	case start.IsZero():
		a.add(Progressing, "%s (since unknown)", what)
	case a.now.Sub(start) > limit:
		a.findings = append(a.findings, Finding{Health: late, Reason: what, Since: &start})
	default:
		a.add(Progressing, "%s", what)
	}
}

func (a *assessor) run() {
	j := a.j
	if !j.DeletingSince.IsZero() {
		a.timed(j.DeletingSince, a.th.StuckAfter, Stuck, "being deleted")
		return
	}
	if j.Lifecycle == "" && j.ReconciledAt.IsZero() {
		a.timed(j.Created, a.th.Grace, Stuck, "not reconciled by the operator yet")
		return
	}
	if j.SpecJobState == "suspended" {
		a.suspended()
		return
	}
	a.operator()
	a.jobManager()
	a.job()
	a.drift()
	a.statusError()
	a.pods()
}

func (a *assessor) suspended() {
	j := a.j
	if j.Lifecycle == "SUSPENDED" {
		a.add(Suspended, "suspended")
	} else {
		a.timed(j.SpecChangedAt, a.th.StuckAfter, Stuck, "suspend requested, lifecycle "+j.Lifecycle)
	}
	a.drift()
}

func (a *assessor) operator() {
	j := a.j
	switch j.Lifecycle {
	case "FAILED":
		// The operator's error says why; it is shown here instead of as a separate finding.
		if j.Error != nil {
			a.add(Failed, "lifecycle FAILED: %s", j.Error.Summary(120))
		} else {
			a.add(Failed, "lifecycle FAILED")
		}
	case "ROLLED_BACK":
		a.add(Degraded, "rolled back: running the last stable spec, not the current one")
	case "UPGRADING", "ROLLING_BACK":
		a.timed(j.ReconciledAt, a.th.StuckAfter, Stuck, "lifecycle "+j.Lifecycle)
	}
}

func (a *assessor) jobManager() {
	j := a.j
	if j.Kind != KindDeployment {
		return
	}
	reconciled := j.ReconciledAt
	// The pod's waiting reason, such as ImagePullBackOff, says why the JobManager is not up.
	waiting := ""
	if j.Pods != nil && j.Pods.JM.Waiting != "" {
		waiting = ", waiting: " + j.Pods.JM.Waiting
	}
	switch j.JMStatus {
	case "ERROR":
		what := "JobManager deployment ERROR"
		if j.Error != nil {
			what += ": " + j.Error.Summary(120)
		} else {
			what += waiting
		}
		a.timed(reconciled, a.th.Grace, Failed, what)
	case "MISSING":
		a.timed(reconciled, a.th.Grace, Failed, "JobManager deployment MISSING")
	case "DEPLOYING":
		a.timed(reconciled, a.th.StuckAfter, Stuck, "JobManager DEPLOYING"+waiting)
	case "DEPLOYED_NOT_READY":
		a.timed(reconciled, a.th.DegradedAfter, Degraded, "JobManager deployed, not ready"+waiting)
	}
}

func (a *assessor) job() {
	j := a.j
	if j.SessionCluster {
		return
	}
	switch j.JobState {
	case "FAILED":
		a.add(Failed, "job FAILED")
	case "FAILING":
		a.add(Failed, "job FAILING")
	case "CANCELED":
		a.add(Degraded, "job CANCELED")
	case "RUNNING", "FINISHED":
	case "":
		a.timed(first(j.ReconciledAt, j.Created), a.th.DegradedAfter, Degraded, "no job status")
	default:
		// RESTARTING, RECONCILING, INITIALIZING, CREATED, CANCELLING, and SUSPENDED by Flink itself (for
		// example after a JobManager failover) while the spec says running.
		a.timed(first(j.JobStateSince, j.ReconciledAt), a.th.DegradedAfter, Degraded, "job "+j.JobState)
	}
}

func (a *assessor) drift() {
	j := a.j
	if !j.Drift() {
		return
	}
	what := fmt.Sprintf("spec generation %d not observed (operator at %d)", j.Generation, j.ObservedGeneration)
	// ReconciledAt is no fallback: it predates the change, so it would make a fresh change look old.
	a.timed(j.SpecChangedAt, a.th.DriftAfter, Stuck, what)
}

// statusError: the operator clears a FlinkDeployment's error on the next healthy observe, so an error under
// STABLE is transient there. A FlinkSessionJob's error persists until the next spec change.
func (a *assessor) statusError() {
	j := a.j
	if j.Error == nil || j.JMStatus == "ERROR" || j.Lifecycle == "FAILED" {
		return
	}
	msg := "operator error: " + j.Error.Summary(120)
	if j.Kind == KindSessionJob && j.Lifecycle == "STABLE" {
		a.add(Stuck, "%s", msg)
		return
	}
	a.timed(j.ReconciledAt, a.th.Grace, Degraded, msg)
}

// pods compares ready TaskManagers with the desired count. It runs only when pods were read, and only for a
// running job, since TaskManagers come and go while a job deploys.
func (a *assessor) pods() {
	j := a.j
	if j.Pods == nil || j.Kind != KindDeployment {
		return
	}
	if j.JobState != "RUNNING" || j.TMReplicas == 0 {
		return
	}
	tm := j.Pods.TM
	if tm.Ready >= int(j.TMReplicas) {
		return
	}
	what := fmt.Sprintf("%d/%d TaskManagers ready", tm.Ready, j.TMReplicas)
	if tm.Waiting != "" {
		what += ", waiting: " + tm.Waiting
	}
	// A missing pod has no not-ready time; the job's last state change is the best bound for it.
	start := tm.NotReadySince
	if tm.Total < int(j.TMReplicas) || start.IsZero() {
		start = latest(j.JobStateSince, j.ReconciledAt)
	}
	a.timed(start, a.th.DegradedAfter, Degraded, what)
}

func latest(ts ...time.Time) time.Time {
	var out time.Time
	for _, t := range ts {
		if t.After(out) {
			out = t
		}
	}
	return out
}
