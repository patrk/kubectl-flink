package tui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
	"github.com/patrk/kubectl-flink/internal/triage/triagetest"
)

const fixtures = "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/"

// fakeLoader serves details collected from captured scenarios and records what it was asked.
type fakeLoader struct {
	mu        sync.Mutex
	details   map[string]*Detail // by job name
	calls     []string
	ctxs      map[string]context.Context
	log       string
	reports   []string
	reportErr error
}

func (f *fakeLoader) Detail(ctx context.Context, job model.Job) (*Detail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, job.Name)
	f.ctxs[job.Name] = ctx
	if d := f.details[job.Name]; d != nil {
		return d, nil
	}
	return nil, errors.New("connection refused")
}

func (f *fakeLoader) Log(context.Context, model.Job, int) (string, error) { return f.log, nil }

func (f *fakeLoader) Report(_ context.Context, job model.Job) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, job.Name)
	if f.reportErr != nil {
		return "", f.reportErr
	}
	return "/tmp/kubectl-flink-report-" + job.Name + ".tar.gz", nil
}

// scenarioJob loads a captured scenario as a job of the scope and its detail, judged with the default rules.
func scenarioJob(t *testing.T, dir string) (model.Job, *Detail, time.Time) {
	t.Helper()
	s := triagetest.Load(t, fixtures+dir)
	ev := s.Evidence()
	v, err := triage.Judge(ev, triage.Signals(ev), triage.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	return *s.Job, &Detail{Evidence: ev, Verdict: v, HistoryErr: errors.New("not built in this test"), Loaded: s.Now}, s.Now
}

// detailModel is top over the snapshots and tm-lost scenarios with a loader serving their details, at the
// capture time of the snapshots scenario.
func detailModel(t *testing.T) (*Model, *fakeLoader) {
	t.Helper()
	snap, snapDetail, at := scenarioJob(t, "snapshots")
	lost, lostDetail, _ := scenarioJob(t, "tm-lost")
	f := &fakeLoader{details: map[string]*Detail{snap.Name: snapDetail, lost.Name: lostDetail}, ctxs: map[string]context.Context{},
		log: "2026-10-24 23:40:00,000 INFO  JobMaster - Job snap switched to RUNNING."}
	scopes := []fleet.Scope{{Jobs: []model.Job{snap, lost}}}
	scopes[0].Context = snap.Context
	m := New(Config{Now: func() time.Time { return at }, Thresholds: model.DefaultThresholds(), Sort: "name", Loader: f})
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.Update(scopesMsg(scopes))
	return m, f
}

// run executes a command and feeds its message back, as the program does; a batch runs each part.
func run(m *Model, cmd tea.Cmd) {
	if cmd == nil {
		return
	}
	switch msg := cmd().(type) {
	case tea.BatchMsg:
		for _, c := range msg {
			run(m, c)
		}
	case nil:
	default:
		_, next := m.Update(msg)
		run(m, next)
	}
}

func TestDetailTabs(t *testing.T) {
	m, _ := detailModel(t)
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter}) // snap is first by name
	run(m, cmd)
	for i, tab := range tabs {
		_, cmd := m.Update(tea.KeyPressMsg{Code: '1' + rune(i), Text: string(rune('1' + i))})
		run(m, cmd)
		golden(t, "detail-"+tab.name, screen(m))
	}
}

// A job whose JobManager cannot be read shows why in each REST tab.
func TestDetailWithoutREST(t *testing.T) {
	m, f := detailModel(t)
	_, d, _ := scenarioJob(t, "missing-jar")
	f.details["snap"] = d
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	run(m, cmd)
	for _, tab := range []string{"2", "3", "4"} {
		press(m, tab)
		golden(t, "detail-no-rest-"+tab, screen(m))
	}
}

func TestDetailLoading(t *testing.T) {
	m, f := detailModel(t)
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if cmd == nil || m.mode != modeDetail {
		t.Fatal("enter collects nothing")
	}
	golden(t, "detail-loading", screen(m))
	run(m, cmd)
	if len(f.calls) != 1 {
		t.Errorf("calls %v", f.calls)
	}
	// Back and in again: the detail is cached.
	press(m, "esc")
	if _, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter}); cmd != nil {
		run(m, cmd)
	}
	if len(f.calls) != 1 {
		t.Errorf("cached detail collected again: %v", f.calls)
	}
	// r collects again.
	_, cmd = m.Update(tea.KeyPressMsg{Code: 'r', Text: "r"})
	run(m, cmd)
	if len(f.calls) != 2 {
		t.Errorf("r: calls %v", f.calls)
	}
}

func TestDetailError(t *testing.T) {
	m, f := detailModel(t)
	delete(f.details, "snap")
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	run(m, cmd)
	golden(t, "detail-error", screen(m))
}

// A prefetch fires only where the cursor rests, and a new collection cancels the one in flight.
func TestPrefetch(t *testing.T) {
	m, f := detailModel(t)
	gen := m.gen
	press(m, "down")
	if m.gen == gen {
		t.Fatal("a move schedules nothing")
	}
	// The tick of an earlier position is stale.
	if cmd := m.prefetch(prefetchMsg{gen: gen}); cmd != nil {
		t.Error("stale prefetch collects")
	}
	// The cursor rested on tm-lost: its collection starts, but is not run yet.
	pending := m.prefetch(prefetchMsg{gen: m.gen})
	if pending == nil || m.loading.key == "" {
		t.Fatal("no prefetch")
	}
	// Moving up and resting on snap cancels tm-lost's collection.
	press(m, "up")
	run(m, m.prefetch(prefetchMsg{gen: m.gen}))
	run(m, pending)
	if ctx := f.ctxs["tm-lost"]; ctx == nil || ctx.Err() == nil {
		t.Error("tm-lost's collection not cancelled")
	}
	if e := m.cache[keyOf(m, "snap")]; e == nil || e.detail == nil {
		t.Error("snap not cached")
	}
	// A cancelled result is dropped, not cached as an error.
	m.collected(detailMsg{key: keyOf(m, "tm-lost"), err: context.Canceled})
	if e := m.cache[keyOf(m, "tm-lost")]; e != nil && e.err != nil {
		t.Errorf("cancelled result cached: %v", e.err)
	}
	// Resting again on a freshly collected job collects nothing.
	press(m, "down", "up")
	if cmd := m.prefetch(prefetchMsg{gen: m.gen}); cmd != nil {
		t.Error("fresh detail collected again")
	}
}

func keyOf(m *Model, name string) string {
	for i := range m.shown {
		if m.shown[i].Job.Name == name {
			return m.shown[i].Job.Context + "\x00" + m.shown[i].Job.Key()
		}
	}
	return ""
}

// y copies the command of the open tab.
func TestDetailCopy(t *testing.T) {
	m, _ := detailModel(t)
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	run(m, cmd)
	press(m, "2")
	if _, cmd := m.Update(tea.KeyPressMsg{Code: 'y', Text: "y"}); cmd == nil {
		t.Fatal("y copies nothing")
	}
	if want := "copied: kubectl flink graph --context kind default/snap"; m.flash != want {
		t.Errorf("flash %q, want %q", m.flash, want)
	}
}

// A JobManager that answered nothing is not reported as a job without failures.
func TestDetailRESTFailed(t *testing.T) {
	m, f := detailModel(t)
	d := *f.details["snap"]
	ev := *d.Evidence
	ev.REST = &triage.REST{}
	ev.Gaps = append(ev.Gaps, triage.Gap{Layer: triage.Runtime, What: "JobManager REST API", Why: "port-forward: connection refused"})
	d.Evidence = &ev
	f.details["snap"] = &d
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	run(m, cmd)
	for _, tab := range []string{"3", "4"} {
		press(m, tab)
		if got := screen(m); !strings.Contains(got, "port-forward: connection refused") || strings.Contains(got, "No failures") {
			t.Errorf("tab %s:\n%s", tab, got)
		}
	}
}

// r collects the job as the watch sees it now, and a list-mode collection reads no log.
func TestDetailRefreshUsesCurrentJob(t *testing.T) {
	m, f := detailModel(t)
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	run(m, cmd)
	scopes := []fleet.Scope{{Jobs: []model.Job{}}}
	scopes[0].Context = m.scopes[0].Context
	for _, j := range m.scopes[0].Jobs {
		if j.Name == "snap" {
			j.JobID = "new-job-id"
		}
		scopes[0].Jobs = append(scopes[0].Jobs, j)
	}
	m.Update(scopesMsg(scopes))
	f.mu.Lock()
	f.ctxs = map[string]context.Context{}
	f.mu.Unlock()
	if got := m.current(); got.JobID != "new-job-id" {
		t.Errorf("current job ID %s", got.JobID)
	}
	// Leave the detail on the Logs tab; a collection that lands in list mode reads no log.
	press(m, "6", "esc")
	m.collected(detailMsg{key: m.detail.key, detail: f.details["snap"]})
	if cmd := m.tabOpened(); cmd != nil {
		t.Error("log read in list mode")
	}
}

// R writes the report of the selected job from the list, and of the open job from its detail; the footer says
// where, and a second R while one is written does not start another.
func TestReport(t *testing.T) {
	m, f := detailModel(t)
	_, cmd := m.Update(tea.KeyPressMsg{Code: 'R', Text: "R"})
	if cmd == nil || !strings.Contains(m.flash, "writing the report of default/snap") {
		t.Fatalf("flash %q", m.flash)
	}
	if _, again := m.Update(tea.KeyPressMsg{Code: 'R', Text: "R"}); again != nil || !strings.Contains(m.flash, "still writing") {
		t.Errorf("second R: %q", m.flash)
	}
	run(m, cmd)
	if m.flash != "report written: /tmp/kubectl-flink-report-snap.tar.gz" || len(f.reports) != 1 {
		t.Errorf("flash %q, reports %v", m.flash, f.reports)
	}
	golden(t, "report-written", screen(m))

	_, cmd = m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	run(m, cmd)
	f.reportErr = errors.New("forbidden")
	_, cmd = m.Update(tea.KeyPressMsg{Code: 'R', Text: "R"})
	run(m, cmd)
	if !strings.Contains(m.flash, "report of default/snap failed: forbidden") {
		t.Errorf("flash %q", m.flash)
	}
}

// w adds ls -o wide's columns and takes them away again.
func TestWide(t *testing.T) {
	m, _ := detailModel(t)
	press(m, "w")
	golden(t, "wide", screen(m))
	press(m, "w")
	if strings.Contains(screen(m), "JM RESTARTS") {
		t.Error("w again keeps the wide columns")
	}
}

// Quitting while a report is written asks first; the second quit drops it.
func TestQuitWhileReporting(t *testing.T) {
	m, _ := detailModel(t)
	m.Update(tea.KeyPressMsg{Code: 'R', Text: "R"}) // not run: still writing
	if _, cmd := m.Update(tea.KeyPressMsg{Code: 'q', Text: "q"}); cmd != nil || !strings.Contains(m.flash, "still being written") {
		t.Fatalf("first q: %q", m.flash)
	}
	if _, cmd := m.Update(tea.KeyPressMsg{Code: 'q', Text: "q"}); cmd == nil {
		t.Error("second q does not quit")
	}
}
