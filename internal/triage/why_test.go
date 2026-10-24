package triage_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

var update = flag.Bool("update", false, "rewrite golden files")

// TestWhyGolden renders why for every captured scenario of every Flink version.
func TestWhyGolden(t *testing.T) {
	dirs, _ := filepath.Glob("../../testdata/fixtures/scenarios-*/*")
	for _, dir := range dirs {
		ev := scenario(t, dir)
		signals := triage.Signals(ev)
		v, err := triage.Judge(ev, signals, triage.DefaultRules())
		if err != nil {
			t.Fatal(err)
		}
		var b bytes.Buffer
		if err := render.Why(&b, ev, v); err != nil {
			t.Fatal(err)
		}
		version := strings.TrimPrefix(filepath.Base(filepath.Dir(dir)), "scenarios-operator-")
		golden(t, "why-"+filepath.Base(dir)+"-"+version, b.Bytes())

		var j bytes.Buffer
		if err := render.JSON(&j, render.NewWhyReport(ev, v, signals)); err != nil {
			t.Fatal(err)
		}
		var back render.WhyReport
		if err := json.Unmarshal(j.Bytes(), &back); err != nil || back.Job.Name != ev.Job.Name || len(back.Signals) == 0 {
			t.Errorf("%s json: %v", dir, err)
		}
	}
}

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s differs:\n--- got\n%s--- want\n%s", name, got, want)
	}
}
