package cli

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
)

func execute(args ...string) (code int, stdout, stderr string) {
	streams, _, out, errOut := genericiooptions.NewTestIOStreams()
	cmd := NewRootCommand(streams)
	cmd.SetArgs(args)
	code = run(cmd, streams)
	return code, out.String(), errOut.String()
}

func TestVersion(t *testing.T) {
	code, out, _ := execute("version")
	if code != ExitOK || !strings.HasPrefix(out, "kubectl-flink dev") {
		t.Fatalf("code %d, output %q", code, out)
	}
}

func TestVersionJSON(t *testing.T) {
	code, out, _ := execute("version", "-o", "json")
	var info map[string]string
	if err := json.Unmarshal([]byte(out), &info); code != ExitOK || err != nil || info["version"] != "dev" {
		t.Fatalf("code %d, err %v, output %q", code, err, out)
	}
}

func TestExitCodes(t *testing.T) {
	tests := []struct {
		args []string
		want int
	}{
		{[]string{"nope"}, ExitUsage},
		{[]string{"version", "--nope"}, ExitUsage},
		{[]string{"version", "extra"}, ExitUsage},
		{[]string{"version", "-o", "yaml"}, ExitUsage},
		{[]string{"--context"}, ExitUsage},
	}
	for _, tt := range tests {
		if code, _, errOut := execute(tt.args...); code != tt.want || !strings.HasPrefix(errOut, "error: ") {
			t.Errorf("%v: code %d, stderr %q, want %d", tt.args, code, errOut, tt.want)
		}
	}
}

func TestKubectlFlags(t *testing.T) {
	cmd := NewRootCommand(genericiooptions.NewTestIOStreamsDiscard())
	for _, f := range []string{"context", "namespace", "kubeconfig"} {
		if cmd.PersistentFlags().Lookup(f) == nil {
			t.Errorf("missing --%s", f)
		}
	}
}

func TestRuntimeErrorIsNotUsage(t *testing.T) {
	// A RunE error whose text resembles cobra's usage messages must still exit with ExitError.
	streams, _, _, _ := genericiooptions.NewTestIOStreams()
	cmd := NewRootCommand(streams)
	cmd.AddCommand(&cobra.Command{Use: "fail", RunE: func(*cobra.Command, []string) error {
		return errors.New("requires a reachable JobManager")
	}})
	markUsageErrors(cmd)
	cmd.SetArgs([]string{"fail"})
	if code := run(cmd, streams); code != ExitError {
		t.Fatalf("code %d, want %d", code, ExitError)
	}
}
