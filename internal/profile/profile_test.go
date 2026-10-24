package profile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/kube"
)

const sample = `apiVersion: kubectl-flink/v1alpha1
profiles:
  - name: prod-jumphost
    match: ["*-prd", "*-pre"]
    reconcileInterval: 30s
    idLabels: [use-case]
    scopes:
      - context: cluster-pre
        namespaces: [team-a, team-b]
      - context: cluster-prd
        namespaces: [team-a]
  - name: dev
    match: ["*"]
    scopes:
      - namespaces: [sandbox]
`

func write(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAndSelect(t *testing.T) {
	f, err := Load(write(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	p := f.For("cluster-prd")
	if p.Name != "prod-jumphost" || p.ReconcileInterval.Duration != 30*time.Second || strings.Join(p.IDLabels, ",") != "use-case" {
		t.Fatalf("got %+v", p)
	}
	if th := p.Thresholds(); th.Grace != time.Minute {
		t.Errorf("grace %s from a 30s interval", th.Grace)
	}
	if p := f.For("cluster-dev"); p.Name != "dev" || p.ReconcileInterval.Duration != time.Minute {
		t.Errorf("fallthrough: %+v", p)
	}
}

func TestMissingFileIsDefault(t *testing.T) {
	f, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if p := f.For("anything"); p.Name != "default" || len(p.Resolve("ctx", Flags{})) != 1 {
		t.Errorf("got %+v", p)
	}
}

func TestLoadRejects(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"version":         {"apiVersion: v9\n", "not supported"},
		"unknown":         {"apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a, match: [x], colour: red}]\n", "unknown field"},
		"no match":        {"apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a}]\n", "match is empty"},
		"dup":             {"apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a, match: [x], scopes: [{context: c}, {context: c}]}]\n", "two scopes"},
		"negative":        {"apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a, match: [x], reconcileInterval: -1s}]\n", "negative"},
		"disable typo":    {"apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a, match: [x], disableRules: [image-pul]}]\n", `no rule "image-pul"`},
		"supersedes typo": {"apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: a, match: [x], rules: [{id: r, layer: kubernetes, severity: OK, title: t, explain: e, when: [{signal: event, match: x}], supersedes: [nope]}]}]\n", `supersedes unknown rule "nope"`},
	} {
		if _, err := Load(write(t, tc.body)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", name, err, tc.want)
		}
	}
}

func scopes(ss []kube.Scope) string {
	b := make([]string, 0, len(ss))
	for _, s := range ss {
		x := s.Context + ":" + strings.Join(s.Namespaces, ",")
		if s.AllNamespaces {
			x += ":all"
		}
		b = append(b, x)
	}
	return strings.Join(b, " ")
}

func TestResolve(t *testing.T) {
	f, err := Load(write(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	prod, dev := f.For("cluster-prd"), f.For("cluster-dev")
	for name, tc := range map[string]struct {
		p       Profile
		current string
		fl      Flags
		want    string
	}{
		"all scopes":            {prod, "cluster-prd", Flags{}, "cluster-pre:team-a,team-b cluster-prd:team-a"},
		"--context narrows":     {prod, "cluster-pre", Flags{Context: "cluster-pre"}, "cluster-pre:team-a,team-b"},
		"-n replaces":           {prod, "cluster-prd", Flags{Namespace: "x"}, "cluster-pre:x cluster-prd:x"},
		"-A widens":             {prod, "cluster-prd", Flags{AllNamespaces: true}, "cluster-pre:team-a,team-b:all cluster-prd:team-a:all"},
		"--context not in it":   {prod, "other", Flags{Context: "other"}, "other:"},
		"scope without context": {dev, "cluster-dev", Flags{}, "cluster-dev:sandbox"},
		"default profile":       {Default(), "kind", Flags{}, "kind:"},
		"same context merged": {Profile{Scopes: []kube.Scope{{Namespaces: []string{"a"}}, {Context: "dev", Namespaces: []string{"a", "b"}}}},
			"dev", Flags{}, "dev:a,b"},
		"merge keeps the context namespace": {Profile{Scopes: []kube.Scope{{}, {Context: "dev", Namespaces: []string{"a"}, AllNamespaces: true}}},
			"dev", Flags{}, "dev:,a:all"},
	} {
		if got := scopes(tc.p.Resolve(tc.current, tc.fl)); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}

func TestGlob(t *testing.T) {
	for pattern, cases := range map[string]map[string]bool{
		"arn:aws:eks:*": {"arn:aws:eks:eu-west-1:123:cluster/prod": true, "gke_p_z_prod": false},
		"*prod*":        {"arn:aws:eks:eu-west-1:123:cluster/prod": true, "gke_p_europe-west1_prod-1": true, "dev": false},
		"*-prd":         {"aks-a-prd": true, "aks-a-prd-2": false},
		"kind":          {"kind": true, "kind-x": false},
		"a*b*c":         {"abc": true, "a-b-c": true, "acb": false, "ab": false},
		"*":             {"": true, "anything/at:all": true},
	} {
		for s, want := range cases {
			if got := glob(pattern, s); got != want {
				t.Errorf("glob(%q, %q) = %v", pattern, s, got)
			}
		}
	}
}

func TestPath(t *testing.T) {
	t.Setenv(EnvPath, "/x/y.yaml")
	if p, _ := Path(); p != "/x/y.yaml" {
		t.Errorf("env: %s", p)
	}
	t.Setenv(EnvPath, "")
	t.Setenv("XDG_CONFIG_HOME", "/xdg")
	if p, _ := Path(); p != "/xdg/kubectl-flink/config.yaml" {
		t.Errorf("xdg: %s", p)
	}
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "/home/u")
	if p, _ := Path(); p != "/home/u/.config/kubectl-flink/config.yaml" {
		t.Errorf("home: %s", p)
	}
}
