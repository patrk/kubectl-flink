package tui

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/charmbracelet/x/exp/teatest/v2"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

// shows reports whether the terminal output holds text once the escape codes are taken out: the renderer sends
// only what changed, so a word can arrive split by cursor and insert codes.
func shows(out []byte, text string) bool { return strings.Contains(ansi.Strip(string(out)), text) }

// The real program loop: scopes arrive on the channel, a key filters, q quits. What the keys did is read from the
// final model, not from the screen, whose updates arrive in pieces.
func TestProgram(t *testing.T) {
	updates := make(chan []fleet.Scope, 1)
	updates <- fleetScopes()
	m := New(Config{Updates: updates, Now: func() time.Time { return now }, Thresholds: model.DefaultThresholds(), Sort: "health"})
	tm := teatest.NewTestModel(t, m, teatest.WithInitialTermSize(120, 16))
	teatest.WaitFor(t, tm.Output(), func(b []byte) bool { return shows(b, "fraud-scoring") }, teatest.WithDuration(10*time.Second))
	tm.Send(tea.KeyPressMsg{Code: 'f', Text: "f"})
	tm.Send(tea.KeyPressMsg{Code: 'q', Text: "q"})
	tm.WaitFinished(t, teatest.WithFinalTimeout(10*time.Second))
	got := tm.FinalModel(t).(*Model)
	if got.filterName() != "unhealthy" || len(got.shown) != 2 {
		t.Errorf("filter %s, %d rows", got.filterName(), len(got.shown))
	}
}

// The output of a CI run where the filter's name arrived split by an insert-mode code.
func TestShows(t *testing.T) {
	out := []byte("\x1b[1;40H2\x1b[1;68H\x1b[K\x1b[2;9Hunhe\x1b[4halthy \x1b[4l\x1b[3;34HHEALTH")
	if !shows(out, "unhealthy") {
		t.Error("split word not found")
	}
}
