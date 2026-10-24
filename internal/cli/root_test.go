package cli

import (
	"encoding/json"
	"strings"
	"testing"

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
