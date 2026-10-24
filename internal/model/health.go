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

// since is the time elapsed from the first non-zero time; -1 if none is known.
func (a *assessor) since(ts ...time.Time) time.Duration {
	for _, t := range ts {
		if !t.IsZero() {
			return a.now.Sub(t)
		}
	}
	return -1
}

// timed adds a finding that is PROGRESSING until d exceeds limit, then late. A negative d means the start is
// unknown; the finding stays PROGRESSING because there is no evidence that it has lasted too long.
func (a *assessor) timed(d, limit time.Duration, late Health, what string) {
	switch {
	case d < 0:
		a.add(Progressing, "%s (since unknown)", what)
	case d > limit:
		a.add(late, "%s for %s", what, duration.HumanDuration(d))
	default:
		a.add(Progressing, "%s", what)
	}
}

func (a *assessor) run() {
	j := a.j
	if !j.DeletingSince.IsZero() {
		a.timed(a.since(j.DeletingSince), a.th.StuckAfter, Stuck, "being deleted")
		return
	}
	if j.Lifecycle == "" && j.ReconciledAt.IsZero() {
		a.timed(a.since(j.Created), a.th.Grace, Stuck, "not reconciled by the operator yet")
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
}

func (a *assessor) suspended() {
	j := a.j
	if j.Lifecycle == "SUSPENDED" {
		a.add(Suspended, "suspended")
	} else {
		a.timed(a.since(j.SpecChangedAt), a.th.StuckAfter, Stuck, "suspend requested, lifecycle "+j.Lifecycle)
	}
	a.drift()
}

func (a *assessor) operator() {
	j := a.j
	switch j.Lifecycle {
	case "FAILED":
		a.add(Failed, "lifecycle FAILED")
	case "ROLLED_BACK":
		a.add(Degraded, "rolled back: running the last stable spec, not the current one")
	case "UPGRADING", "ROLLING_BACK":
		a.timed(a.since(j.ReconciledAt), a.th.StuckAfter, Stuck, "lifecycle "+j.Lifecycle)
	}
}

func (a *assessor) jobManager() {
	j := a.j
	if j.Kind != KindDeployment {
		return
	}
	reconciled := a.since(j.ReconciledAt)
	switch j.JMStatus {
	case "ERROR":
		what := "JobManager deployment ERROR"
		if j.Error != nil {
			what += ": " + j.Error.Summary(120)
		}
		a.timed(reconciled, a.th.Grace, Failed, what)
	case "MISSING":
		a.timed(reconciled, a.th.Grace, Failed, "JobManager deployment MISSING")
	case "DEPLOYING":
		a.timed(reconciled, a.th.StuckAfter, Stuck, "JobManager DEPLOYING")
	case "DEPLOYED_NOT_READY":
		a.timed(reconciled, a.th.DegradedAfter, Degraded, "JobManager deployed, not ready")
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
		a.timed(a.since(j.ReconciledAt, j.Created), a.th.DegradedAfter, Degraded, "no job status")
	default:
		// RESTARTING, RECONCILING, INITIALIZING, CREATED, CANCELLING, and SUSPENDED by Flink itself (for
		// example after a JobManager failover) while the spec says running.
		a.timed(a.since(j.JobStateSince, j.ReconciledAt), a.th.DegradedAfter, Degraded, "job "+j.JobState)
	}
}

func (a *assessor) drift() {
	j := a.j
	if !j.Drift() {
		return
	}
	what := fmt.Sprintf("spec generation %d not observed (operator at %d)", j.Generation, j.ObservedGeneration)
	// ReconciledAt is no fallback: it predates the change, so it would make a fresh change look old.
	a.timed(a.since(j.SpecChangedAt), a.th.DriftAfter, Stuck, what)
}

// statusError: the operator clears a FlinkDeployment's error on the next healthy observe, so an error under
// STABLE is transient there. A FlinkSessionJob's error persists until the next spec change.
func (a *assessor) statusError() {
	j := a.j
	if j.Error == nil || j.JMStatus == "ERROR" {
		return
	}
	msg := "operator error: " + j.Error.Summary(120)
	if j.Kind == KindSessionJob && j.Lifecycle == "STABLE" {
		a.add(Stuck, "%s", msg)
		return
	}
	a.timed(a.since(j.ReconciledAt), a.th.Grace, Degraded, msg)
}
