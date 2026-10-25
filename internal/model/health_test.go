package model

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/kube"
)

var now = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

func ago(d time.Duration) time.Time { return now.Add(-d) }

// running is a healthy application job; cases override what they test.
func running() Job {
	return Job{
		Kind: KindDeployment, Namespace: "ns", Name: "job", Generation: 3, ObservedGeneration: 3,
		Created: ago(48 * time.Hour), Lifecycle: "STABLE", JobState: "RUNNING", JMStatus: "READY",
		ReconcileState: "DEPLOYED", JobStateSince: ago(time.Hour), ReconciledAt: ago(time.Hour),
		SpecChangedAt: ago(2 * time.Hour), SpecJobState: "running",
	}
}

func tms(total, ready int, notReadySince time.Time) *ClusterPods {
	return &ClusterPods{
		JM: Component{Total: 1, Ready: 1},
		TM: Component{Total: total, Ready: ready, NotReadySince: notReadySince},
	}
}

func TestAssess(t *testing.T) {
	tests := []struct {
		name   string
		edit   func(*Job)
		want   Health
		reason string
	}{
		{"running", func(*Job) {}, OK, ""},
		{"deployed not yet stable but running", func(j *Job) { j.Lifecycle = "DEPLOYED" }, OK, ""},
		{"finished batch job", func(j *Job) { j.JobState = "FINISHED" }, OK, ""},
		{"lifecycle failed", func(j *Job) { j.Lifecycle = "FAILED" }, Failed, "lifecycle FAILED"},
		{"lifecycle failed with error", func(j *Job) {
			j.Lifecycle, j.Error = "FAILED", ParseStatusError(`{"type":"a.B","message":"savepoint not found"}`)
		}, Failed, "lifecycle FAILED: savepoint not found"},
		{"job failed", func(j *Job) { j.JobState = "FAILED" }, Failed, "job FAILED"},
		{"job failing", func(j *Job) { j.JobState = "FAILING" }, Failed, "job FAILING"},
		{"JM error after grace", func(j *Job) {
			j.JMStatus, j.JobState, j.Error = "ERROR", "RECONCILING", ParseStatusError(`{"type":"x.DeploymentFailedException","message":"back-off restarting","additionalMetadata":{"reason":"CrashLoopBackOff"}}`)
		}, Failed, "JobManager deployment ERROR: CrashLoopBackOff: back-off restarting for 60m"},
		{"JM error at startup", func(j *Job) {
			j.JMStatus, j.JobState, j.ReconciledAt, j.JobStateSince = "ERROR", "RECONCILING", ago(20*time.Second), ago(20*time.Second)
		}, Progressing, "JobManager deployment ERROR"},
		{"JM missing", func(j *Job) { j.JMStatus = "MISSING" }, Failed, "JobManager deployment MISSING for 60m"},
		{"JM deploying too long", func(j *Job) { j.JMStatus = "DEPLOYING" }, Stuck, "JobManager DEPLOYING for 60m"},
		{"JM not ready too long", func(j *Job) { j.JMStatus = "DEPLOYED_NOT_READY" }, Degraded, "not ready for 60m"},
		{"JM not ready briefly", func(j *Job) { j.JMStatus, j.ReconciledAt = "DEPLOYED_NOT_READY", ago(time.Minute) }, Progressing, "not ready"},
		{"restarting long", func(j *Job) { j.JobState = "RESTARTING" }, Degraded, "job RESTARTING for 60m"},
		{"restarting briefly", func(j *Job) { j.JobState, j.JobStateSince = "RESTARTING", ago(30*time.Second) }, Progressing, "job RESTARTING"},
		{"reconciling long", func(j *Job) { j.JobState = "RECONCILING" }, Degraded, "job RECONCILING for 60m"},
		{"canceled", func(j *Job) { j.JobState = "CANCELED" }, Degraded, "job CANCELED"},
		{"rolled back", func(j *Job) { j.Lifecycle = "ROLLED_BACK" }, Degraded, "rolled back"},
		{"upgrading too long", func(j *Job) { j.Lifecycle = "UPGRADING" }, Stuck, "lifecycle UPGRADING for 60m"},
		{"upgrading briefly", func(j *Job) { j.Lifecycle, j.ReconciledAt = "UPGRADING", ago(2*time.Minute) }, Progressing, "lifecycle UPGRADING"},
		{"rolling back too long", func(j *Job) { j.Lifecycle = "ROLLING_BACK" }, Stuck, "ROLLING_BACK"},
		{"drift old", func(j *Job) { j.Generation, j.SpecChangedAt = 4, ago(20*time.Minute) }, Stuck, "spec generation 4 not observed (operator at 3) for 20m"},
		{"drift fresh", func(j *Job) { j.Generation, j.SpecChangedAt = 4, ago(time.Minute) }, Progressing, "spec generation 4"},
		{"error while stable, deployment", func(j *Job) { j.Error = ParseStatusError("Job Not Found") }, Degraded, "operator error: Job Not Found"},
		{"error while stable, session job", func(j *Job) {
			j.Kind, j.JMStatus, j.Error = KindSessionJob, "", ParseStatusError(`{"type":"a.B","message":"boom"}`)
		}, Stuck, "operator error: boom"},
		{"suspended", func(j *Job) {
			j.SpecJobState, j.Lifecycle, j.JobState, j.JMStatus = "suspended", "SUSPENDED", "FINISHED", "MISSING"
		}, Suspended, "suspended"},
		{"suspended, then changed", func(j *Job) {
			j.SpecJobState, j.Lifecycle, j.JobState, j.JMStatus = "suspended", "SUSPENDED", "FINISHED", "MISSING"
			j.Generation, j.ObservedGeneration, j.SpecChangedAt = 4, 3, ago(time.Hour)
		}, Suspended, "suspended; spec changes apply on resume"},
		{"suspend pending", func(j *Job) { j.SpecJobState, j.SpecChangedAt = "suspended", ago(time.Minute) }, Progressing, "suspend requested"},
		{"suspend stuck", func(j *Job) { j.SpecJobState, j.SpecChangedAt = "suspended", ago(time.Hour) }, Stuck, "suspend requested, lifecycle STABLE for 60m"},
		{"never reconciled, new", func(j *Job) { j.Lifecycle, j.ReconciledAt, j.Created = "", time.Time{}, ago(10*time.Second) }, Progressing, "not reconciled"},
		{"never reconciled, old", func(j *Job) { j.Lifecycle, j.ReconciledAt = "", time.Time{} }, Stuck, "not reconciled"},
		{"deleting", func(j *Job) { j.DeletingSince, j.Lifecycle = ago(time.Minute), "FAILED" }, Progressing, "being deleted"},
		{"deletion stuck", func(j *Job) { j.DeletingSince = ago(2 * time.Hour) }, Stuck, "being deleted for 120m"},
		{"suspended by Flink", func(j *Job) { j.JobState = "SUSPENDED" }, Degraded, "job SUSPENDED for 60m"},
		{"unknown job state", func(j *Job) { j.JobState, j.JobStateSince = "SOMETHING_NEW", ago(time.Minute) }, Progressing, "job SOMETHING_NEW"},
		{"drift with unknown start", func(j *Job) { j.Generation, j.SpecChangedAt = 4, time.Time{} }, Progressing, "(since unknown)"},
		{"suspend with unknown start", func(j *Job) { j.SpecJobState, j.SpecChangedAt = "suspended", time.Time{} }, Progressing, "(since unknown)"},
		{"error during startup", func(j *Job) {
			j.JMStatus, j.ReconciledAt, j.Error = "DEPLOYING", ago(30*time.Second), ParseStatusError("starting")
		}, Progressing, "operator error: starting"},
		{"session cluster ready", func(j *Job) { j.SessionCluster, j.JobState = true, "" }, OK, ""},
		{"session cluster missing JM", func(j *Job) { j.SessionCluster, j.JobState, j.JMStatus = true, "", "MISSING" }, Failed, "MISSING"},
		{"worst finding wins", func(j *Job) { j.JobState, j.Lifecycle = "RESTARTING", "FAILED" }, Failed, "lifecycle FAILED"},
		{"TaskManagers ready", func(j *Job) { j.TMReplicas, j.Pods = 2, tms(2, 2, time.Time{}) }, OK, ""},
		{"pods unknown", func(j *Job) { j.TMReplicas = 2 }, OK, ""},
		{"TaskManager not ready long", func(j *Job) { j.TMReplicas, j.Pods = 2, tms(2, 1, ago(20*time.Minute)) }, Degraded, "1/2 TaskManagers ready for 20m"},
		{"TaskManager not ready briefly", func(j *Job) { j.TMReplicas, j.Pods = 2, tms(2, 1, ago(time.Minute)) }, Progressing, "1/2 TaskManagers ready"},
		{"TaskManager pod missing", func(j *Job) { j.TMReplicas, j.Pods = 2, tms(1, 1, time.Time{}) }, Degraded, "1/2 TaskManagers ready for 60m"},
		{"TaskManager waiting", func(j *Job) {
			j.TMReplicas, j.Pods = 2, tms(2, 1, ago(time.Hour))
			j.Pods.TM.Waiting = "CrashLoopBackOff (job-taskmanager-1-2)"
		}, Degraded, "waiting: CrashLoopBackOff (job-taskmanager-1-2)"},
		{"TaskManagers while restarting", func(j *Job) { j.JobState, j.TMReplicas, j.Pods = "RESTARTING", 2, tms(2, 0, ago(time.Hour)) }, Degraded, "job RESTARTING"},
		{"session job ignores cluster TaskManagers", func(j *Job) {
			j.Kind, j.JMStatus, j.TMReplicas, j.Pods = KindSessionJob, "", 2, tms(2, 0, ago(time.Hour))
		}, OK, ""},
		{"JobManager waiting while deploying", func(j *Job) {
			j.JMStatus, j.ReconciledAt = "DEPLOYING", ago(3*time.Minute)
			j.Pods = &ClusterPods{JM: Component{Total: 1, Waiting: "ImagePullBackOff (job-abc)"}}
		}, Progressing, "JobManager DEPLOYING, waiting: ImagePullBackOff (job-abc)"},
		{"JobManager waiting, not ready too long", func(j *Job) {
			j.JMStatus = "DEPLOYED_NOT_READY"
			j.Pods = &ClusterPods{JM: Component{Total: 1, Waiting: "CrashLoopBackOff (job-abc)"}}
		}, Degraded, "JobManager deployed, not ready, waiting: CrashLoopBackOff (job-abc) for 60m"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := running()
			tt.edit(&j)
			got := Assess(&j, now, DefaultThresholds())
			if got.Health != tt.want {
				t.Fatalf("health %s, want %s; findings %+v", got.Health, tt.want, got.Findings)
			}
			if tt.reason == "" {
				if len(got.Findings) != 0 {
					t.Fatalf("unexpected findings %+v", got.Findings)
				}
				return
			}
			// The reason must be among the findings with the job's health, which sort first.
			for _, f := range got.Findings {
				if f.Health == got.Health && strings.Contains(f.Text(now), tt.reason) {
					return
				}
			}
			t.Fatalf("no %s finding contains %q: %+v", got.Health, tt.reason, got.Findings)
		})
	}
}

func loadJob(t *testing.T, path string) Job {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fd kube.FlinkDeployment
	if err := json.Unmarshal(b, &fd); err != nil {
		t.Fatal(err)
	}
	return FromDeployment("kind", &fd)
}

// loadPods reads a captured pod list; nil when the fixture has none.
func loadPods(t *testing.T, path string) []kube.Pod {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var l kube.PodList
	if err := json.Unmarshal(b, &l); err != nil {
		t.Fatal(err)
	}
	return l.Items
}

// Fixtures are real operator output; health is checked at the capture time plus an offset.
func TestAssessFixtures(t *testing.T) {
	const dir = "../../testdata/fixtures/"
	tests := []struct {
		path  string
		after time.Duration
		want  Health
	}{
		{"operator-1.15.0-flink-1.20/flinkdeployment.json", time.Minute, OK},
		{"operator-1.15.0-flink-2.2/flinkdeployment.json", time.Minute, OK},
		{"operator-1.16.1-flink-1.20/flinkdeployment.json", time.Minute, OK},
		{"operator-1.16.1-flink-2.2/flinkdeployment.json", time.Minute, OK},
		{"scenarios-operator-1.16.1-flink-2.2/suspended/flinkdeployment.json", time.Minute, Suspended},
		{"scenarios-operator-1.16.1-flink-2.2/bad-image/flinkdeployment.json", 10 * time.Minute, Failed},
		{"scenarios-operator-1.16.1-flink-2.2/missing-jar/flinkdeployment.json", 10 * time.Minute, Failed},
	}
	for _, tt := range tests {
		j := loadJob(t, dir+tt.path)
		if pods := loadPods(t, filepath.Join(filepath.Dir(dir+tt.path), "pods.json")); pods != nil {
			jobs := []Job{j}
			AttachPods(jobs, SummarisePods(pods), func(string) bool { return true })
			j = jobs[0]
		}
		at := j.ReconciledAt.Add(tt.after)
		if got := Assess(&j, at, DefaultThresholds()); got.Health != tt.want {
			t.Errorf("%s: %s, want %s; findings %+v", tt.path, got.Health, tt.want, got.Findings)
		}
	}
}

func TestFromDeploymentOwners(t *testing.T) {
	j := loadJob(t, "../../testdata/fixtures/scenarios-operator-1.16.1-flink-2.2/suspended/flinkdeployment.json")
	if strings.Join(j.SpecOwners, ",") != "kubectl-client-side-apply,kubectl-patch" {
		t.Errorf("owners %v", j.SpecOwners)
	}
	if j.SpecChangedAt.IsZero() || j.SpecJobState != "suspended" || j.Lifecycle != "SUSPENDED" {
		t.Errorf("job %+v", j)
	}
}

func TestThresholdsFollowReconcileInterval(t *testing.T) {
	if d := DefaultThresholds(); d.Grace != 2*time.Minute || d.DegradedAfter != 5*time.Minute || d.StuckAfter != 10*time.Minute {
		t.Errorf("defaults %+v", d)
	}
	slow := ThresholdsFor(5 * time.Minute)
	if slow.Grace != 10*time.Minute || slow.DriftAfter != 25*time.Minute || slow.StuckAfter != 50*time.Minute {
		t.Errorf("5m interval %+v", slow)
	}
	// A 30 minute drift is STUCK at the default interval but still within bounds for a slow operator.
	j := running()
	j.Generation, j.SpecChangedAt = 4, ago(20*time.Minute)
	if h := Assess(&j, now, slow).Health; h != Progressing {
		t.Errorf("slow operator: %s", h)
	}
}

func TestHealthTextRoundTrip(t *testing.T) {
	for h := OK; h <= Failed; h++ {
		b, _ := h.MarshalText()
		var back Health
		if err := back.UnmarshalText(b); err != nil || back != h {
			t.Errorf("%s: %s %v", h, back, err)
		}
	}
	var h Health
	if err := h.UnmarshalText([]byte("SAD")); err == nil {
		t.Error("want error")
	}
}

func TestParseHealth(t *testing.T) {
	for h := OK; h <= Failed; h++ {
		if got, err := ParseHealth(strings.ToLower(h.String())); err != nil || got != h {
			t.Errorf("%s: %v %v", h, got, err)
		}
	}
	if _, err := ParseHealth("fine"); err == nil {
		t.Error("want error")
	}
	if b, _ := json.Marshal(Assessment{Health: Stuck}); string(b) != `{"health":"STUCK"}` {
		t.Errorf("json %s", b)
	}
}

func TestStatusError(t *testing.T) {
	e := ParseStatusError(`{"type":"org.apache.flink.kubernetes.operator.exception.ReconciliationException",
		"message":"outer","throwableList":[{"type":"java.net.ConnectException","message":"Connection refused\nat x"}],
		"additionalMetadata":{"httpResponseCode":503}}`)
	if e.Root().Type != "java.net.ConnectException" || e.HTTPCode != 503 || e.Summary(0) != "Connection refused" {
		t.Errorf("parsed %+v summary %q", e, e.Summary(0))
	}
	if e := ParseStatusError(`{"type":"a.b.OnlyType"}`); e.Summary(0) != "OnlyType" {
		t.Errorf("type-only summary %q", e.Summary(0))
	}
	if e := ParseStatusError("Job Not Found"); e.Raw != "Job Not Found" || e.Summary(0) != "Job Not Found" {
		t.Errorf("plain %+v", e)
	}
	if e := ParseStatusError("{broken"); e.Raw != "{broken" {
		t.Errorf("broken json %+v", e)
	}
	if ParseStatusError("  ") != nil {
		t.Error("empty must be nil")
	}
	if got := truncate("abcdef", 4); got != "abc…" {
		t.Errorf("truncate %q", got)
	}
}
