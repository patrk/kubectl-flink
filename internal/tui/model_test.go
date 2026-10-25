package tui

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

var now = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

func ago(d time.Duration) time.Time { return now.Add(-d) }

func job(ctx, ns, name string, edit func(*model.Job)) model.Job {
	j := model.Job{
		Kind: model.KindDeployment, Context: ctx, Namespace: ns, Name: name, Lifecycle: "STABLE", JobState: "RUNNING",
		JMStatus: "READY", Generation: 1, ObservedGeneration: 1, ReconciledAt: ago(3 * time.Hour),
		JobStateSince: ago(3 * time.Hour), Created: ago(48 * time.Hour), SpecJobState: "running",
	}
	if edit != nil {
		edit(&j)
	}
	return j
}

// fleetScopes is two contexts with jobs in several namespaces and states, and a third that cannot be read.
func fleetScopes() []fleet.Scope {
	tmDown := &model.ClusterPods{
		JM:  model.Component{Total: 1, Ready: 1},
		TM:  model.Component{Total: 2, Ready: 1, NotReadySince: ago(20 * time.Minute)},
		CPU: resource.MustParse("1"), Memory: resource.MustParse("2Gi"),
	}
	dev := []model.Job{
		job("dev", "team-a", "orders", nil),
		job("dev", "team-a", "payments", func(j *model.Job) { j.TMReplicas, j.Pods = 2, tmDown }),
		job("dev", "team-b", "replay", func(j *model.Job) {
			j.Lifecycle, j.JobState, j.JobStateSince = "FAILED", "FAILED", ago(12*time.Minute)
			j.Error = model.ParseStatusError(`{"type":"org.apache.flink.FooException","message":"java.net.ConnectException: Connection refused: kafka-0.kafka:9092"}`)
		}),
		job("dev", "team-b", "nightly", func(j *model.Job) {
			j.SpecJobState, j.Lifecycle, j.JobState = "suspended", "SUSPENDED", "SUSPENDED"
		}),
		job("dev", "payments", "fraud-scoring", func(j *model.Job) { j.Labels = map[string]string{"use-case": "4711"} }),
	}
	prd := []model.Job{job("prd", "payments", "fraud-scoring", nil)}
	scopes := []fleet.Scope{{Jobs: dev}, {Jobs: prd}, {Err: errors.New("context broken: Unauthorized")}}
	scopes[0].Context, scopes[1].Context, scopes[2].Context = "dev", "prd", "broken"
	scopes[0].AllNamespaces = true
	return scopes
}

// newModel is top at a fixed size and time with the scopes loaded.
func newModel(t *testing.T, scopes []fleet.Scope, width, height int) *Model {
	t.Helper()
	m := New(Config{Now: func() time.Time { return now }, Thresholds: model.DefaultThresholds(), Sort: "health"})
	m.Update(tea.WindowSizeMsg{Width: width, Height: height})
	m.Update(scopesMsg(scopes))
	return m
}

var special = map[string]tea.Key{
	"enter": {Code: tea.KeyEnter}, "esc": {Code: tea.KeyEscape}, "down": {Code: tea.KeyDown}, "up": {Code: tea.KeyUp},
	"ctrl+f": {Code: 'f', Mod: tea.ModCtrl},
}

// press sends keys: a name from special, or text typed one rune at a time.
func press(m *Model, keys ...string) {
	for _, k := range keys {
		if sk, ok := special[k]; ok {
			m.Update(tea.KeyPressMsg(sk))
			continue
		}
		for _, r := range k {
			m.Update(tea.KeyPressMsg{Code: r, Text: string(r)})
		}
	}
}

// screen is what the terminal shows, without colours, with trailing spaces trimmed.
func screen(m *Model) string {
	lines := strings.Split(ansi.Strip(m.View().Content), "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " ")
	}
	return strings.Join(lines, "\n") + "\n"
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if f := flag.Lookup("update"); f != nil && f.Value.String() == "true" { // registered by teatest
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path) // #nosec G304 -- test golden
	if err != nil {
		t.Fatalf("%v (run with -update)", err)
	}
	if got != string(want) {
		t.Errorf("%s differs:\n%s", name, got)
	}
}

func TestScreens(t *testing.T) {
	for name, keys := range map[string][]string{
		"list":            nil,
		"filter":          {"f"},
		"filter-failed":   {"f", "f"},
		"sort":            {"s", "s"},
		"search":          {"/", "4711"},
		"search-kept":     {"/", "team-b", "enter"},
		"help":            {"?"},
		"namespace":       {"n", "pay"},
		"namespace-pick":  {"n", "pay", "enter"},
		"context-command": {":", "ctx prd", "enter"},
		"unknown-command": {":", "bogus", "enter"},
		"enter":           {"down", "enter"},
		"no-match":        {"/", "nothing-like-this"},
		"esc-clears":      {"f", "/", "team", "enter", "esc"},
	} {
		t.Run(name, func(t *testing.T) {
			m := newModel(t, fleetScopes(), 120, 16)
			press(m, keys...)
			golden(t, "top-"+name, screen(m))
		})
	}
}

// A narrow terminal cuts every line, the reason column first, and stacks the help.
func TestNarrow(t *testing.T) {
	m := newModel(t, fleetScopes(), 60, 12)
	golden(t, "top-narrow", screen(m))
	press(m, "?")
	golden(t, "top-narrow-help", screen(m))
}

// Colours: health cells in their colour, the cursor row reversed.
func TestColors(t *testing.T) {
	m := newModel(t, fleetScopes(), 120, 12)
	press(m, "down")
	golden(t, "top-colors", m.View().Content+"\n")
}

func TestLoading(t *testing.T) {
	m := New(Config{Now: func() time.Time { return now }})
	m.Update(tea.WindowSizeMsg{Width: 80, Height: 10})
	if got := screen(m); !strings.Contains(got, "loading Flink jobs") {
		t.Errorf("loading screen %q", got)
	}
}

// The cursor stays on its job when new data re-sorts the rows under it.
func TestCursorFollowsJob(t *testing.T) {
	m := newModel(t, fleetScopes(), 120, 16)
	press(m, "down", "down", "down")
	want := m.selected().Job.Name
	scopes := fleetScopes()
	scopes[0].Jobs[0].JobState, scopes[0].Jobs[0].Lifecycle = "FAILED", "FAILED" // orders jumps to the top
	m.Update(scopesMsg(scopes))
	if got := m.selected(); got == nil || got.Job.Name != want {
		t.Errorf("cursor on %v, want %s", got, want)
	}
	// A job that goes away leaves the cursor at the same position.
	pos := m.table.cursor
	scopes = fleetScopes()
	for i := range scopes[0].Jobs {
		if scopes[0].Jobs[i].Name == want {
			scopes[0].Jobs = append(scopes[0].Jobs[:i], scopes[0].Jobs[i+1:]...)
			break
		}
	}
	m.Update(scopesMsg(scopes))
	if m.table.cursor != pos {
		t.Errorf("cursor at %d, want %d", m.table.cursor, pos)
	}
}

func TestQuit(t *testing.T) {
	for _, keys := range [][]string{{"q"}, {":", "q", "enter"}} {
		m := newModel(t, fleetScopes(), 120, 16)
		press(m, keys[:len(keys)-1]...)
		last := tea.KeyPressMsg{Code: tea.KeyEnter}
		if k := keys[len(keys)-1]; k != "enter" {
			last = tea.KeyPressMsg{Code: rune(k[0]), Text: k}
		}
		_, cmd := m.Update(last)
		if cmd == nil {
			t.Fatalf("%v: no command", keys)
		}
		if _, ok := cmd().(tea.QuitMsg); !ok {
			t.Errorf("%v: not quit", keys)
		}
	}
	// In search, q is text.
	m := newModel(t, fleetScopes(), 120, 16)
	press(m, "/", "q")
	if m.search != "q" {
		t.Errorf("search %q", m.search)
	}
}

// When the watch ends, top ends.
func TestClosedUpdates(t *testing.T) {
	ch := make(chan []fleet.Scope)
	close(ch)
	m := New(Config{Updates: ch, Now: func() time.Time { return now }})
	if _, ok := m.wait()().(closedMsg); !ok {
		t.Fatal("closed channel not reported")
	}
	if _, cmd := m.Update(closedMsg{}); cmd == nil {
		t.Fatal("no quit")
	}
}

// --unhealthy starts at the unhealthy step of f, so the first f goes on to FAILED.
func TestStartFilter(t *testing.T) {
	m := New(Config{Now: func() time.Time { return now }, Health: fleet.UnhealthyValues()})
	m.Update(scopesMsg(fleetScopes()))
	if m.filterName() != "unhealthy" {
		t.Fatalf("filter %s", m.filterName())
	}
	press(m, "f")
	if m.filterName() != "FAILED" {
		t.Errorf("after f: %s", m.filterName())
	}
	m = New(Config{Now: func() time.Time { return now }, Health: []model.Health{model.Failed, model.Stuck}})
	if m.filterName() != "FAILED,STUCK" {
		t.Errorf("custom filter %s", m.filterName())
	}
}

// When the only scope cannot be read, top ends with its error, as ls does.
func TestOnlyScopeFails(t *testing.T) {
	m := New(Config{Now: func() time.Time { return now }})
	broken := []fleet.Scope{{Err: errors.New("context dev: Unauthorized")}}
	if _, cmd := m.Update(scopesMsg(broken)); cmd == nil || m.err == nil {
		t.Fatalf("no quit, err %v", m.err)
	}
	// Once loaded, a failing scope is a row, not the end.
	m = newModel(t, fleetScopes()[:1], 120, 16)
	if _, cmd := m.Update(scopesMsg(broken)); cmd == nil || m.err != nil {
		t.Errorf("err %v", m.err)
	}
}

// A namespace that matches nothing keeps the filter in force.
func TestPickerNoMatch(t *testing.T) {
	m := newModel(t, fleetScopes(), 120, 16)
	press(m, ":", "ns team-a", "enter", "n", "nothing", "enter")
	if m.namespace != "team-a" {
		t.Errorf("namespace %q", m.namespace)
	}
}

// The status line never runs past the screen, and a warning comes before the filters.
func TestStatusLineFits(t *testing.T) {
	scopes := fleetScopes()
	scopes[0].WatchErr = errors.New("watch failing: connection refused")
	m := newModel(t, scopes, 80, 16)
	press(m, "/", "a-very-long-search-text", "enter", ":", "ns some-long-namespace-name", "enter")
	line := strings.Split(screen(m), "\n")[1]
	if ansi.StringWidth(line) > 80 || !strings.HasPrefix(line, "context dev: watch failing") {
		t.Errorf("status line %q", line)
	}
}
