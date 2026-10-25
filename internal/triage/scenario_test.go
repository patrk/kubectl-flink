package triage_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
	"github.com/patrk/kubectl-flink/internal/triage/triagetest"
)

// scenario collects a captured scenario's evidence the way why does, at the time of the capture.
func scenario(t *testing.T, dir string) *triage.Evidence {
	t.Helper()
	return triagetest.Load(t, dir).Evidence()
}

func jobOf(t *testing.T, fd map[string]any) model.Job { return triagetest.JobOf(t, fd) }

func judge(t *testing.T, ev *triage.Evidence) *triage.Verdict {
	t.Helper()
	v, err := triage.Judge(ev, triage.Signals(ev), triage.DefaultRules())
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func ids(v *triage.Verdict) string {
	out := make([]string, 0, len(v.Findings))
	for _, f := range v.Findings {
		out = append(out, f.ID)
	}
	return strings.Join(out, ",")
}

// TestScenarioVerdicts is M2's done criterion on captured failures: why names the induced cause first, on every
// captured Flink version.
func TestScenarioVerdicts(t *testing.T) {
	// Health is what ls shows and runs on timers, so failures captured within the grace period are still
	// PROGRESSING; the rules already name them.
	want := map[string]struct {
		first  string
		health model.Health
	}{
		"bad-image":          {"image-pull", model.Progressing},
		"missing-jar":        {"jar-missing", model.Progressing},
		"missing-secret":     {"mount-failed", model.Progressing},
		"unschedulable":      {"unschedulable", model.Progressing},
		"bad-memory":         {"memory-config", model.Progressing},
		"tm-lost":            {"taskmanager-lost", model.OK},
		"suspended":          {"", model.Suspended},
		"unreachable":        {"dependency-unreachable", model.Failed},
		"restart-loop":       {"dependency-unreachable", model.OK},
		"restart-bot":        {"external-restart", model.OK},
		"ha":                 {"", model.OK},
		"checkpoint-storage": {"checkpoint-storage-unwritable", model.Progressing},
		"snapshots":          {"", model.OK},
		"savepoint-failing":  {"savepoint-dir-missing", model.OK},
		"snapshot-ignored":   {"snapshots-ignored", model.OK},
	}
	dirs, _ := filepath.Glob("../../testdata/fixtures/scenarios-*/*")
	if len(dirs) < 2*len(want) {
		t.Fatalf("found %d scenario fixtures, want %d", len(dirs), 2*len(want))
	}
	for _, dir := range dirs {
		name := filepath.Base(dir)
		w, ok := want[name]
		if !ok {
			t.Errorf("no expectation for scenario %s", dir)
			continue
		}
		ev := scenario(t, dir)
		v := judge(t, ev)
		first := ""
		if len(v.Findings) > 0 {
			first = v.Findings[0].ID
		}
		if first != w.first {
			t.Errorf("%s: findings %s, want %s first; signals %+v", dir, ids(v), w.first, triage.Signals(ev))
		}
		if v.Health != w.health {
			t.Errorf("%s: health %s, want %s; %+v", dir, v.Health, w.health, ev.Health.Findings)
		}
	}
}
