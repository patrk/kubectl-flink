package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/patrk/kubectl-flink/internal/triage/triagetest"
)

const haScenario = "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/ha"

// copyDir copies a fixture directory so a test can change it.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o750)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func editJSON(t *testing.T, path string, edit func(map[string]any)) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	edit(v)
	if b, err = json.Marshal(v); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func m(v any) map[string]any { return v.(map[string]any) }

// plantCanaries puts a unique marker in every place a secret is known to turn up; a marker in the bundle names
// the place that leaked.
func plantCanaries(t *testing.T, dir string) {
	editJSON(t, filepath.Join(dir, "flinkdeployment.json"), func(fd map[string]any) {
		spec := m(fd["spec"])
		cfg := m(spec["flinkConfiguration"])
		cfg["s3.secret-key"] = "CANARY-FLAT-CONFIG"
		cfg["security"] = map[string]any{"ssl": map[string]any{"keystore-password": "CANARY-NESTED-CONFIG", "keystore": "/etc/ks.jks"}}
		m(spec["job"])["args"] = []any{"--password", "CANARY-ARG-PAIR", "--db.token=CANARY-ARG-INLINE"}
		env := []any{map[string]any{"name": "DB_PASSWORD", "value": "CANARY-POD-TEMPLATE-ENV"},
			map[string]any{"name": "CREDS", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "app-creds", "key": "password"}}}}
		spec["podTemplate"] = map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "flink-main-container", "env": env}},
			"volumes": []any{map[string]any{"name": "creds", "secret": map[string]any{"secretName": "app-creds"}}}}}
		m(spec["jobManager"])["podTemplate"] = map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "flink-main-container",
			"env": []any{map[string]any{"name": "API_TOKEN", "value": "CANARY-JM-TEMPLATE-ENV"}}}}}}
		md := m(fd["metadata"])
		ann, _ := md["annotations"].(map[string]any)
		if ann == nil {
			ann = map[string]any{}
			md["annotations"] = ann
		}
		ann["kubectl.kubernetes.io/last-applied-configuration"] = `{"spec":{"flinkConfiguration":{"s3.secret-key":"CANARY-LAST-APPLIED"}}}`
		st := m(fd["status"])
		st["error"] = "cannot connect to jdbc:postgresql://db/x?user=app&password=CANARY-STATUS-ERROR"
		m(st["reconciliationStatus"])["lastReconciledSpec"] = `{"spec":{"flinkConfiguration":{"s3.secret-key":"CANARY-LAST-RECONCILED","taskmanager.numberOfTaskSlots":"1"}}}`
	})
	editJSON(t, filepath.Join(dir, "pods.json"), func(l map[string]any) {
		pod := m(l["items"].([]any)[0])
		c := m(m(pod["spec"])["containers"].([]any)[0])
		c["env"] = append(c["env"].([]any), map[string]any{"name": "AWS_SECRET_ACCESS_KEY", "value": "CANARY-POD-ENV"})
		c["args"] = []any{"--s3.secret-key", "CANARY-POD-ARG"}
	})
	editJSON(t, filepath.Join(dir, "events.json"), func(l map[string]any) {
		e := m(l["items"].([]any)[0])
		e["message"] = "request failed: Authorization: Bearer CANARY-EVENT-BEARER-0123456789"
	})
	editJSON(t, filepath.Join(dir, "configmaps.json"), func(l map[string]any) {
		for _, item := range l["items"].([]any) {
			cm := m(item)
			if strings.HasPrefix(m(cm["metadata"])["name"].(string), "flink-config-") {
				cm["data"] = map[string]any{"config.yaml": "taskmanager.numberOfTaskSlots: 1\ns3.secret-key: CANARY-CONFIGMAP-YAML\n",
					"log4j-console.properties": "appender.x.password=CANARY-CONFIGMAP-PROPERTIES\n"}
			}
		}
	})
	logs, _ := filepath.Glob(filepath.Join(dir, "logs", "*.txt"))
	for _, l := range logs {
		f, err := os.OpenFile(l, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString(`INFO sasl.jaas.config=PlainLoginModule required username="app" password="CANARY-LOG-JAAS";` + "\n" +
			"WARN fetching https://deploy:CANARY-LOG-URL@repo.example.com/app.jar\n")
		_ = f.Close()
	}
	b, err := os.ReadFile(filepath.Join(dir, "rest", "jobmanager-config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := make([]any, 0, 64)
	_ = json.Unmarshal(b, &cfg)
	cfg = append(cfg, map[string]any{"key": "s3.access-key", "value": "CANARY-REST-JM-CONFIG"})
	b, _ = json.Marshal(cfg)
	if err := os.WriteFile(filepath.Join(dir, "rest", "jobmanager-config.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	editJSON(t, filepath.Join(dir, "rest", "job-config.json"), func(c map[string]any) {
		m(c["execution-config"])["user-config"] = map[string]any{"db.password": "CANARY-REST-USER-CONFIG", "db.host": "db.internal"}
	})
}

var canary = regexp.MustCompile(`CANARY-[A-Z-]+`)

// TestReportRedactsEveryKnownLocation is the redaction guarantee of the report bundle.
func TestReportRedactsEveryKnownLocation(t *testing.T) {
	dir := copyDir(t, haScenario)
	plantCanaries(t, dir)
	s := triagetest.Load(t, dir)
	c := newCluster(t)
	c.add("dev", "default", s.API)
	out := t.TempDir()
	code, stdout, errOut := c.run("report", "default/ha", "--rest-url", s.RESTURL, "--dir", out)
	if code != ExitOK {
		t.Fatalf("%d %s", code, errOut)
	}
	var all strings.Builder
	files := 0
	err := filepath.WalkDir(out, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files++
		b, err := os.ReadFile(path)
		for _, leak := range canary.FindAllString(string(b), -1) {
			rel, _ := filepath.Rel(out, path)
			t.Errorf("%s leaked into %s", leak, rel)
		}
		all.Write(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if leak := canary.FindString(stdout); leak != "" {
		t.Errorf("%s leaked into the printed summary", leak)
	}
	// What must survive: key names, references to secrets and ordinary settings.
	for _, kept := range []string{`"s3.secret-key": "[REDACTED]"`, `"secretName": "app-creds"`, `"key": "password"`,
		"taskmanager.numberOfTaskSlots", `"/etc/ks.jks"`, `"db.host": "db.internal"`, `username="app"`} {
		if !strings.Contains(all.String(), kept) {
			t.Errorf("%s missing from the bundle", kept)
		}
	}
	if files < 10 {
		t.Errorf("only %d files", files)
	}
}

// runAt runs the CLI against a scenario at its capture time, for stable ages in goldens.
func runAt(t *testing.T, s *triagetest.Scenario, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	c := newCluster(t)
	c.add("dev", "default", s.API)
	streams, _, out, errOut := genericiooptions.NewTestIOStreams()
	o := &Options{ConfigFlags: genericclioptions.NewConfigFlags(true), IOStreams: streams, Now: func() time.Time { return s.Now }}
	cmd := newRootCommand(o)
	cmd.SetArgs(append([]string{"--kubeconfig", c.kubeconfig}, args...))
	code = run(cmd, streams)
	return code, out.String(), errOut.String()
}

func TestReportGolden(t *testing.T) {
	for _, name := range []string{"missing-jar", "missing-secret", "tm-lost"} {
		s := triagetest.Load(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/"+name)
		out := t.TempDir()
		args := []string{"report", "default/" + name, "--dir", out}
		if s.RESTURL != "" {
			args = append(args, "--rest-url", s.RESTURL)
		}
		code, stdout, errOut := runAt(t, s, args...)
		if code != ExitOK {
			t.Fatalf("%s: %d %s", name, code, errOut)
		}
		// The REST URL of the fake JobManager changes from run to run.
		if s.RESTURL != "" {
			stdout = strings.ReplaceAll(stdout, s.RESTURL, "http://jobmanager")
		}
		golden(t, "report-summary-"+name, stdout)
		if name == "missing-secret" {
			b, err := os.ReadFile(filepath.Join(out, "events.json"))
			if err != nil || !strings.Contains(string(b), `secret \"does-not-exist\" not found`) {
				t.Errorf("the missing secret's name must survive: %v", err)
			}
		}
	}
}

// The bundle carries the job's FlinkStateSnapshots, with the error the operator recorded.
func TestReportSnapshots(t *testing.T) {
	s := triagetest.Load(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/savepoint-failing")
	out := t.TempDir()
	if code, _, errOut := runAt(t, s, "report", "default/basic", "--dir", out); code != ExitOK {
		t.Fatalf("%d %s", code, errOut)
	}
	b, err := os.ReadFile(filepath.Join(out, "snapshots.json"))
	if err != nil || !strings.Contains(string(b), "basic-savepoint") || !strings.Contains(string(b), "savepoint-dir") {
		t.Errorf("snapshots.json: %v\n%s", err, b)
	}
}

func TestReportTarball(t *testing.T) {
	s := triagetest.Load(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/missing-jar")
	dir := t.TempDir()
	sums := make([]string, 0, 2)
	for i := range 2 {
		path := filepath.Join(dir, fmt.Sprintf("r%d.tar.gz", i))
		code, stdout, errOut := runAt(t, s, "report", "missing-jar", "--file", path, "-q")
		if code != ExitOK || strings.TrimSpace(stdout) != path || !strings.Contains(errOut, "wrote "+path) {
			t.Fatalf("%d %q %s", code, stdout, errOut)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sums = append(sums, fmt.Sprintf("%x", sha256.Sum256(b)))
		if i == 0 {
			names := tarNames(t, b)
			top := "kubectl-flink-report-default-missing-jar-" + s.Now.UTC().Format("20060102T150405Z") + "/"
			if !slices.IsSorted(names) || !slices.Contains(names, top+"summary.md") || !slices.Contains(names, top+"why.txt") ||
				!slices.ContainsFunc(names, func(n string) bool { return strings.HasPrefix(n, top+"logs/") }) {
				t.Errorf("entries %v", names)
			}
		}
	}
	if sums[0] != sums[1] {
		t.Error("two reports of the same capture differ")
	}
	// An existing file is never overwritten.
	if code, _, errOut := runAt(t, s, "report", "missing-jar", "--file", filepath.Join(dir, "r0.tar.gz")); code != ExitError {
		t.Errorf("overwrite: %d %s", code, errOut)
	}
}

func tarNames(t *testing.T, b []byte) []string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	var names []string
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return names
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
}

func TestHistoryCommand(t *testing.T) {
	s := triagetest.Load(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/restart-bot")
	code, out, errOut := runAt(t, s, "history", "restart-bot")
	if code != ExitOK || !strings.Contains(out, "SpecChanged: UPGRADE change(s) detected (Diff: FlinkDeploymentSpec[restartNonce : 1 -> 2])") ||
		!strings.Contains(out, "created, generation 1") || !strings.Contains(out, "Events are kept for about an hour") {
		t.Errorf("%d %s\n%s", code, errOut, out)
	}
	golden(t, "history-restart-bot", out)
	code, out, _ = runAt(t, s, "history", "restart-bot", "-o", "json")
	var h struct {
		Entries []struct{ Source string } `json:"entries"`
	}
	if err := json.Unmarshal([]byte(out), &h); code != ExitOK || err != nil || len(h.Entries) < 5 {
		t.Errorf("json: %d %v", code, err)
	}
}
