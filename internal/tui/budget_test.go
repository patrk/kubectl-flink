//go:build perf

package tui

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

// bigFleet is 1,500 jobs over 4 namespaces, a tenth of them failing.
func bigFleet() []fleet.Scope {
	jobs := make([]model.Job, 0, 1500)
	for i := range 1500 {
		jobs = append(jobs, job("prd", fmt.Sprintf("team-%d", i%4), fmt.Sprintf("job-%04d", i), func(j *model.Job) {
			j.Labels = map[string]string{"use-case": fmt.Sprint(10000 + i)}
			if i%10 == 0 {
				j.Lifecycle, j.JobState = "FAILED", "FAILED"
				j.Error = model.ParseStatusError(`{"type":"x.FooException","message":"java.net.ConnectException: Connection refused"}`)
			}
		}))
	}
	s := []fleet.Scope{{Jobs: jobs}}
	s[0].Context, s[0].AllNamespaces = "prd", true
	return s
}

// budget fails the test when the median of runs of fn is over limit.
func budget(t *testing.T, name string, limit time.Duration, fn func()) {
	t.Helper()
	times := make([]time.Duration, 0, 21)
	for range 21 {
		start := time.Now()
		fn()
		times = append(times, time.Since(start))
	}
	slices.Sort(times)
	median := times[len(times)/2]
	t.Logf("%s: median %v (budget %v)", name, median, limit)
	if median > limit {
		t.Errorf("%s: median %v over budget %v", name, median, limit)
	}
}

// A frame renders only the rows in view, so its cost does not grow with the fleet.
func TestBudgetFrame(t *testing.T) {
	m := newModel(t, bigFleet(), 200, 60)
	budget(t, "frame, 1500 jobs", 16*time.Millisecond, func() {
		press(m, "down")
		_ = m.View()
	})
	budget(t, "frame after page down", 16*time.Millisecond, func() {
		press(m, "ctrl+f")
		_ = m.View()
	})
}

// New data, a filter or a search re-derives every row once; that is the cost of each watch publish.
func TestBudgetRefresh(t *testing.T) {
	m := newModel(t, bigFleet(), 200, 60)
	scopes := bigFleet()
	budget(t, "refresh on new data, 1500 jobs", 50*time.Millisecond, func() { m.Update(scopesMsg(scopes)) })
	budget(t, "search keystroke", 50*time.Millisecond, func() {
		press(m, "/", "1")
		press(m, "esc")
	})
}
