package kube

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/cli-runtime/pkg/genericclioptions"
)

const kubeconfig = `apiVersion: v1
kind: Config
current-context: dev
clusters:
- name: dev
  cluster: {server: "https://dev.example:6443"}
- name: prd
  cluster: {server: "https://prd.example:6443"}
users:
- name: u
  user: {token: t}
contexts:
- name: dev
  context: {cluster: dev, user: u, namespace: team-a}
- name: prd
  context: {cluster: prd, user: u}
`

func TestContexts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	flags := genericclioptions.NewConfigFlags(false)
	flags.KubeConfig = &path
	server := "https://override.example:6443"
	flags.APIServer = &server

	cs, err := NewContexts(flags)
	if err != nil {
		t.Fatal(err)
	}
	if cs.Current() != "dev" {
		t.Errorf("current %q", cs.Current())
	}
	cfg, ns, err := cs.Config("")
	if err != nil || cfg.Host != server || ns != "team-a" {
		t.Errorf("current: host %s ns %s err %v; flags must apply", cfg.Host, ns, err)
	}
	cfg, ns, err = cs.Config("prd")
	if err != nil || cfg.Host != "https://prd.example:6443" || ns != "default" || cfg.BearerToken != "t" {
		t.Errorf("prd: host %s ns %s err %v", cfg.Host, ns, err)
	}
	if _, _, err := cs.Config("missing"); err == nil {
		t.Error("want error for unknown context")
	}

	// kubectx and kubens only rewrite current-context and the context's namespace; both are picked up.
	switched := strings.Replace(strings.Replace(kubeconfig, "current-context: dev", "current-context: prd", 1),
		"{cluster: prd, user: u}", "{cluster: prd, user: u, namespace: team-b}", 1)
	if err := os.WriteFile(path, []byte(switched), 0o600); err != nil {
		t.Fatal(err)
	}
	flags = genericclioptions.NewConfigFlags(false)
	flags.KubeConfig = &path
	cs, err = NewContexts(flags)
	if err != nil {
		t.Fatal(err)
	}
	if cfg, ns, err := cs.Config(""); err != nil || cs.Current() != "prd" || ns != "team-b" || cfg.Host != "https://prd.example:6443" {
		t.Errorf("kubectx/kubens: current %s ns %s err %v", cs.Current(), ns, err)
	}

	ctx := "dev"
	flags.Context = &ctx
	cs, err = NewContexts(flags)
	if err != nil || cs.Current() != "dev" {
		t.Errorf("--context: %q %v", cs.Current(), err)
	}
}
