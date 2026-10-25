package model

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/patrk/kubectl-flink/internal/kube"
)

func TestFailuresWithin(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	j := &Job{Failures: []Failure{
		// Merged for 10 hours, 2,000 times: about 50 in the last 15 minutes.
		{First: ago(10 * time.Hour), Last: ago(time.Minute), Count: 2000},
		// Three within the window.
		{First: ago(5 * time.Minute), Last: ago(2 * time.Minute), Count: 3},
		// Over before the window.
		{First: ago(2 * time.Hour), Last: ago(20 * time.Minute), Count: 400},
		// Once, inside.
		{First: ago(time.Minute), Last: ago(time.Minute), Count: 1},
	}}
	// 2000 × 14m/599m ≈ 46.7, + 3 + 1.
	if got := j.FailuresWithin(now, RestartWindow); got != 51 {
		t.Errorf("failures in 15m: %d", got)
	}
	if got := (&Job{}).FailuresWithin(now, RestartWindow); got != 0 {
		t.Errorf("no events: %d", got)
	}
	// No first time: counted in full, not spread over the years since the zero time.
	if got := (&Job{Failures: []Failure{{Last: ago(time.Minute), Count: 40}}}).FailuresWithin(now, RestartWindow); got != 40 {
		t.Errorf("no first time: %d", got)
	}
	// A clock ahead of ours does not make the window longer.
	skewed := &Job{Failures: []Failure{{First: ago(30 * time.Minute), Last: now.Add(15 * time.Minute), Count: 30}}}
	if got := skewed.FailuresWithin(now, RestartWindow); got != 15 {
		t.Errorf("skewed clock: %d", got)
	}
}

func TestRestartLoopHealth(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	base := Job{Kind: KindDeployment, UID: "u1", Lifecycle: "STABLE", JMStatus: "READY", Generation: 1, ObservedGeneration: 1,
		ReconciledAt: now.Add(-time.Hour), SpecJobState: "running"}
	events := []kube.Event{
		{InvolvedObject: kube.ObjectReference{UID: types.UID("u1")}, Reason: "JobException", Type: "Warning", Count: 4,
			FirstTimestamp: metav1.NewTime(now.Add(-6 * time.Minute)), LastTimestamp: metav1.NewTime(now.Add(-30 * time.Second))},
		// Another object's, with the same name long gone.
		{InvolvedObject: kube.ObjectReference{UID: types.UID("u0")}, Reason: "JobException", Type: "Warning", Count: 99,
			FirstTimestamp: metav1.NewTime(now.Add(-6 * time.Minute)), LastTimestamp: metav1.NewTime(now.Add(-30 * time.Second))},
	}
	for state, want := range map[string]Health{"RESTARTING": Failed, "RUNNING": Degraded, "FAILED": Failed, "CANCELED": Degraded} {
		jobs := []Job{base}
		jobs[0].JobState, jobs[0].JobStateSince = state, now.Add(-20*time.Second)
		AttachFailures(jobs, events)
		a := Assess(&jobs[0], now, DefaultThresholds())
		loop := len(a.Findings) > 0 && a.Findings[0].Reason == "restart loop: 4 failures in the last 15m"
		if a.Health != want || loop != (state == "RESTARTING" || state == "RUNNING") {
			t.Errorf("%s: %s %+v", state, a.Health, a.Findings)
		}
	}
	// A burst that began a minute ago is DEGRADED while the job restarts: it may still recover.
	jobs := []Job{base}
	jobs[0].JobState = "RESTARTING"
	jobs[0].Failures = []Failure{{First: now.Add(-time.Minute), Last: now.Add(-10 * time.Second), Count: 3}}
	if a := Assess(&jobs[0], now, DefaultThresholds()); a.Health != Degraded {
		t.Errorf("burst: %s %+v", a.Health, a.Findings)
	}
	// Failures that ended before the window leave a running job healthy.
	jobs = []Job{base}
	jobs[0].JobState = "RUNNING"
	jobs[0].Failures = []Failure{{First: now.Add(-time.Hour), Last: now.Add(-20 * time.Minute), Count: 30}}
	if a := Assess(&jobs[0], now, DefaultThresholds()); a.Health != OK {
		t.Errorf("recovered: %s %+v", a.Health, a.Findings)
	}
}
