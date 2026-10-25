package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/profile"
)

func TestConfigView(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", kubetest.New(t))
	code, out, errOut := c.run("config", "view", "-o", "json")
	var v EffectiveConfig
	if err := json.Unmarshal([]byte(out), &v); code != ExitOK || err != nil {
		t.Fatalf("%d %v %s", code, err, errOut)
	}
	if v.Profile != "default" || v.FileExists || v.Context != "dev" || len(v.Scopes) != 1 || v.Scopes[0].Context != "dev" || v.Thresholds.Grace != "2m0s" || v.Rules.Total == 0 {
		t.Errorf("%+v", v)
	}

	prof := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(prof, []byte(`apiVersion: kubectl-flink/v1alpha1
profiles:
  - name: team
    match: ["dev"]
    idLabels: [use-case]
    reconcileInterval: 30s
    disableRules: [image-pull]
    scopes: [{context: dev, namespaces: [team-a, team-b]}, {context: gone}]
    rules:
      - {id: own, layer: runtime, severity: OK, title: t, explain: e, when: [{signal: event, match: x}]}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(profile.EnvPath, prof)
	code, out, _ = c.run("config", "view")
	for _, want := range []string{"profile: team", "fileExists: true", "- team-a", "- use-case", "grace: 1m0s", "- image-pull", "- own",
		"context: gone", "error: context gone not found in kubeconfig"} {
		if code != ExitOK || !strings.Contains(out, want) {
			t.Errorf("%q missing:\n%s", want, out)
		}
	}
	if code, _, _ := c.run("config", "view", "-o", "table"); code != ExitUsage {
		t.Errorf("bad output: %d", code)
	}
}

func TestConfigValidate(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", kubetest.New(t))
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := write("good.yaml", "apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a, match: ['*'], scopes: [{context: dev}, {context: prd}]}]\n")
	code, out, errOut := c.run("config", "validate", "-f", good)
	if code != ExitOK || !strings.Contains(out, "is valid: 1 profiles (a)") || !strings.Contains(errOut, `context "prd" is not in the kubeconfig`) {
		t.Errorf("good: %d %q %q", code, out, errOut)
	}
	bad := write("bad.yaml", "apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a, match: ['*'], disableRules: [image-pul]}]\n")
	if code, _, errOut := c.run("config", "validate", "-f", bad); code != ExitError || !strings.Contains(errOut, `no rule "image-pul"`) {
		t.Errorf("bad: %d %q", code, errOut)
	}
	if code, _, errOut := c.run("config", "validate", "-f", filepath.Join(dir, "absent.yaml")); code != ExitError || !strings.Contains(errOut, "no profile at") {
		t.Errorf("absent: %d %q", code, errOut)
	}
	// An unreadable kubeconfig is said, not skipped.
	c.kubeconfig = filepath.Join(dir, "no-such-kubeconfig")
	if _, _, errOut := c.run("config", "validate", "-f", good); !strings.Contains(errOut, "contexts not checked") {
		t.Errorf("unreadable kubeconfig: %q", errOut)
	}
}

func TestCompletion(t *testing.T) {
	c := newCluster(t)
	c.add("dev", "team-a", healthyAndFailed(t))
	// Shells keep candidates that extend the word: namespace/name by its prefix, or the bare name by its own.
	code, out, _ := c.run("__complete", "why", "team-a/brok")
	if code != ExitOK || !strings.Contains(out, "team-a/broken-0\n") || strings.Contains(out, "team-a/ok-0") {
		t.Errorf("namespace/name: %d %q", code, out)
	}
	if _, out, _ = c.run("__complete", "why", "brok"); !strings.HasPrefix(out, "broken-0\n") {
		t.Errorf("bare name: %q", out)
	}
	if _, out, _ = c.run("__complete", "why", "oken"); strings.Contains(out, "broken") {
		t.Errorf("a substring is not offered: %q", out)
	}
	if _, out, _ = c.run("__complete", "why", "-A", ""); !strings.Contains(out, "team-b/other-0") {
		t.Errorf("-A: %q", out)
	}
	if _, out, _ = c.run("__complete", "--context", ""); !strings.Contains(out, "dev\n") {
		t.Errorf("contexts: %q", out)
	}
	if _, out, _ = c.run("__complete", "why", "team-a/broken-0", ""); strings.Contains(out, "team-a/") {
		t.Errorf("second argument: %q", out)
	}
	if code, out, _ = c.run("completion", "zsh"); code != ExitOK || !strings.Contains(out, "#compdef") {
		t.Errorf("zsh: %d", code)
	}
}
