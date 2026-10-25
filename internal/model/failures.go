package model

import (
	"math"
	"time"

	"github.com/patrk/kubectl-flink/internal/kube"
)

// A job with RestartLoop or more failures within RestartWindow is restarting in a loop.
const (
	RestartLoop   = 3
	RestartWindow = 15 * time.Minute
)

// AttachFailures gives each job its JobException events, matched by UID: events of an earlier object with the
// same name linger after it is deleted.
func AttachFailures(jobs []Job, events []kube.Event) {
	byUID := map[string][]Failure{}
	for i := range events {
		e := &events[i]
		first := e.FirstTimestamp.Time
		if first.IsZero() {
			first = e.EventTime.Time
		}
		uid := string(e.InvolvedObject.UID)
		byUID[uid] = append(byUID[uid], Failure{First: first, Last: e.Last(), Count: e.Times()})
	}
	for i := range jobs {
		if f := byUID[jobs[i].UID]; f != nil {
			jobs[i].Failures = f
		}
	}
}

// FailuresWithin estimates how many times the job failed in the window before now. The operator merges a
// repeating exception into one event with a count, so an event that began before the window counts in
// proportion to the part of its span inside the window: its failures are taken to be spread evenly.
// An event without a first time counts in full, and a last time ahead of now, from a skewed clock, is taken as now.
func (j *Job) FailuresWithin(now time.Time, window time.Duration) int {
	n, _ := j.failuresWithin(now, window)
	return n
}

// failuresWithin is FailuresWithin with the earliest time inside the window that a counted failure may have
// happened at.
func (j *Job) failuresWithin(now time.Time, window time.Duration) (int, time.Time) {
	from := now.Add(-window)
	n, earliest := 0.0, time.Time{}
	for _, f := range j.Failures {
		last := f.Last
		if last.After(now) {
			last = now
		}
		if last.Before(from) {
			continue
		}
		start := f.First
		switch {
		case start.IsZero() || f.Count == 1 || !start.Before(from):
			n += float64(f.Count)
			if start.Before(from) {
				start = from
			}
		default:
			n += float64(f.Count) * last.Sub(from).Seconds() / last.Sub(start).Seconds()
			start = from
		}
		if start.IsZero() || start.After(last) {
			start = last
		}
		if earliest.IsZero() || start.Before(earliest) {
			earliest = start
		}
	}
	return int(math.Round(n)), earliest
}
