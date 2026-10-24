package kube

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func decodeFixture(t *testing.T, path string) FlinkDeployment {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fd FlinkDeployment
	if err := json.Unmarshal(b, &fd); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return fd
}

func TestDecodeFixtures(t *testing.T) {
	paths, _ := filepath.Glob("../../testdata/fixtures/*/flinkdeployment.json")
	scenarios, _ := filepath.Glob("../../testdata/fixtures/scenarios-*/*/flinkdeployment.json")
	paths = append(paths, scenarios...)
	if len(paths) < 4 {
		t.Fatalf("found %d fixtures, want at least 4", len(paths))
	}
	for _, p := range paths {
		fd := decodeFixture(t, p)
		if fd.Name == "" || fd.Namespace == "" || fd.Generation == 0 || fd.CreationTimestamp.IsZero() {
			t.Errorf("%s: metadata not decoded: %+v", p, fd.Metadata)
		}
		if fd.Status.LifecycleState == "" || fd.Status.ReconciliationStatus.ReconciliationTimestamp == 0 {
			t.Errorf("%s: status not decoded: %+v", p, fd.Status)
		}
		if fd.Spec.Job == nil || fd.Spec.FlinkVersion == "" || fd.Spec.FlinkConfiguration["taskmanager.numberOfTaskSlots"] != "1" {
			t.Errorf("%s: spec not decoded: %+v", p, fd.Spec)
		}
		if _, ok := fd.Annotations[lastAppliedAnnotation]; ok {
			t.Errorf("%s: last-applied-configuration kept", p)
		}
	}
}

func TestDecodeRunning(t *testing.T) {
	fd := decodeFixture(t, "../../testdata/fixtures/operator-1.16.1-flink-2.2/flinkdeployment.json")
	s := fd.Status
	if s.LifecycleState != "STABLE" || s.JobStatus.State != "RUNNING" || s.JobManagerDeploymentStatus != "READY" {
		t.Fatalf("states %s/%s/%s", s.LifecycleState, s.JobStatus.State, s.JobManagerDeploymentStatus)
	}
	if s.JobStatus.StartTime == 0 || s.JobStatus.UpdateTime < s.JobStatus.StartTime {
		t.Errorf("job times %d %d", s.JobStatus.StartTime, s.JobStatus.UpdateTime)
	}
	if s.TaskManager == nil || s.TaskManager.Replicas != 1 || len(s.Conditions) != 1 || s.Conditions[0].Type != "Running" {
		t.Errorf("taskManager %+v conditions %+v", s.TaskManager, s.Conditions)
	}
}

func TestDecodeManagers(t *testing.T) {
	fd := decodeFixture(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/suspended/flinkdeployment.json")
	var patch *Manager
	for i := range fd.Managers {
		if fd.Managers[i].Name == "kubectl-patch" {
			patch = &fd.Managers[i]
		}
	}
	if patch == nil || strings.Join(patch.SpecFields, ",") != "job" || patch.Time.IsZero() {
		t.Fatalf("kubectl-patch manager %+v in %+v", patch, fd.Managers)
	}
	if fd.Spec.Job.State != "suspended" {
		t.Errorf("job state %q", fd.Spec.Job.State)
	}
}

func TestFlinkConfigFlattens(t *testing.T) {
	var c FlinkConfig
	in := `{"taskmanager.numberOfTaskSlots": 2, "high-availability": {"type": "kubernetes", "storageDir": "s3://b/ha"},
		"pipeline.jars": ["a.jar", "b.jar"], "empty": null, "flag": true,
		"taskmanager.memory.process.size": 1073741824, "big": 9007199254740993, "ratio": 0.25}`
	if err := json.Unmarshal([]byte(in), &c); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"taskmanager.numberOfTaskSlots":   "2",
		"high-availability.type":          "kubernetes",
		"high-availability.storageDir":    "s3://b/ha",
		"pipeline.jars":                   "a.jar;b.jar",
		"empty":                           "",
		"flag":                            "true",
		"taskmanager.memory.process.size": "1073741824",
		"big":                             "9007199254740993",
		"ratio":                           "0.25",
	}
	if len(c) != len(want) {
		t.Fatalf("got %v", c)
	}
	for k, v := range want {
		if c[k] != v {
			t.Errorf("%s = %q, want %q", k, c[k], v)
		}
	}
}

func TestConfigMapKeysOnly(t *testing.T) {
	var cm ConfigMap
	in := `{"metadata":{"name":"x-cluster-config-map"},"data":{"jobGraph-b3":"aGVsbG8=","counter":"12"}}`
	if err := json.Unmarshal([]byte(in), &cm); err != nil {
		t.Fatal(err)
	}
	if strings.Join(cm.Keys, ",") != "counter,jobGraph-b3" {
		t.Errorf("keys %v", cm.Keys)
	}
	b, err := json.Marshal(&cm)
	var back ConfigMap
	if err != nil || json.Unmarshal(b, &back) != nil || strings.Join(back.Keys, ",") != "counter,jobGraph-b3" {
		t.Errorf("round trip: %s %v", b, err)
	}
}

func TestEpochMilli(t *testing.T) {
	for in, want := range map[string]EpochMilli{`"1792847434122"`: 1792847434122, `1792847405384`: 1792847405384, `""`: 0, `null`: 0} {
		var e EpochMilli
		if err := json.Unmarshal([]byte(in), &e); err != nil || e != want {
			t.Errorf("%s: %d, %v", in, e, err)
		}
	}
	var e EpochMilli
	if err := json.Unmarshal([]byte(`"soon"`), &e); err == nil {
		t.Error("want error for non-numeric value")
	}
}
