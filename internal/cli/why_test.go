package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrk/kubectl-flink/internal/kube/kubetest"
	"github.com/patrk/kubectl-flink/internal/profile"
	"github.com/patrk/kubectl-flink/internal/render"
)

const badImage = "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/bad-image"

func badImageCluster(t *testing.T) *cluster {
	f := kubetest.New(t)
	f.Put("flinkdeployments", kubetest.ReadFixture(t, filepath.Join(badImage, "flinkdeployment.json")))
	f.PutAll("pods", filepath.Join(badImage, "pods.json"))
	f.PutAll("events", filepath.Join(badImage, "events.json"))
	c := newCluster(t)
	c.add("dev", "default", f)
	return c
}

func TestWhy(t *testing.T) {
	c := badImageCluster(t)
	code, out, errOut := c.run("why", "bad-image", "--exit-code")
	if code != ExitUnhealthy || !strings.Contains(out, "1. FAILED: The image flink:does-not-exist cannot be pulled (kubernetes, newest evidence") ||
		!strings.Contains(out, "not collected: JobManager REST API: not asked: the JobManager is ERROR") {
		t.Errorf("%d %s\n%s", code, errOut, out)
	}
	code, out, _ = c.run("why", "default/bad-image", "-o", "json")
	var rep render.WhyReport
	if err := json.Unmarshal([]byte(out), &rep); code != ExitOK || err != nil || len(rep.Verdict.Findings) == 0 ||
		rep.Verdict.Findings[0].ID != "image-pull" || rep.Job.Name != "bad-image" {
		t.Errorf("json: %d %v\n%s", code, err, out)
	}
}

func TestWhyProfileRules(t *testing.T) {
	c := badImageCluster(t)
	prof := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(prof, []byte(`apiVersion: kubectl-flink/v1alpha1
profiles:
  - name: team
    match: ["*"]
    disableRules: [image-pull]
    rules:
      - id: registry-mirror
        layer: kubernetes
        severity: FAILED
        when: [{signal: event, match: 'failed to pull and unpack image "(?P<image>[^"]+)"'}]
        title: 'Image {{.image}} is not in the mirror'
        explain: 'Images must be pushed to the team registry first.'
        remedy: 'Run the publish pipeline.'
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(profile.EnvPath, prof)
	code, out, errOut := c.run("why", "bad-image")
	if code != ExitOK || !strings.Contains(out, "Image docker.io/library/flink:does-not-exist is not in the mirror") ||
		strings.Contains(out, "cannot be pulled") {
		t.Errorf("%d %s\n%s", code, errOut, out)
	}
	// A broken profile rule is reported, not ignored.
	if err := os.WriteFile(prof, []byte("apiVersion: kubectl-flink/v1alpha1\nprofiles: [{name: team, match: ['*'], rules: [{id: x, layer: kubernetes, severity: FAILED, title: t, explain: e, when: [{signal: nope, match: x}]}]}]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, errOut := c.run("why", "bad-image"); code != ExitError || !strings.Contains(errOut, `unknown signal "nope"`) ||
		!strings.Contains(errOut, "config.yaml") {
		t.Errorf("broken rule: %d %s", code, errOut)
	}
}
