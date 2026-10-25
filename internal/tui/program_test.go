package tui

import (
	"bytes"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/exp/teatest/v2"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

// The real program loop: scopes arrive on the channel, a key filters, q quits.
func TestProgram(t *testing.T) {
	updates := make(chan []fleet.Scope, 1)
	updates <- fleetScopes()
	m := New(Config{Updates: updates, Now: func() time.Time { return now }, Thresholds: model.DefaultThresholds(), Sort: "health"})
	tm := teatest.NewTestModel(t, m, teatest.WithInitialTermSize(120, 16))
	teatest.WaitFor(t, tm.Output(), func(b []byte) bool { return bytes.Contains(b, []byte("fraud-scoring")) },
		teatest.WithDuration(5*time.Second))
	tm.Send(tea.KeyPressMsg{Code: 'f', Text: "f"})
	teatest.WaitFor(t, tm.Output(), func(b []byte) bool { return bytes.Contains(b, []byte("unhealthy")) },
		teatest.WithDuration(5*time.Second))
	tm.Send(tea.KeyPressMsg{Code: 'q', Text: "q"})
	tm.WaitFinished(t, teatest.WithFinalTimeout(5*time.Second))
	if got := tm.FinalModel(t).(*Model); got.filterName() != "unhealthy" {
		t.Errorf("filter %s", got.filterName())
	}
}
