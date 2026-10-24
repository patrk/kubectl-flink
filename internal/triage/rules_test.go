package triage_test

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
)

var job = &model.Job{Namespace: "team-a", Name: "orders"}

// synthetic are signals for failures kind cannot induce cheaply; every other default rule must fire on a
// captured fixture.
var synthetic = map[string][]triage.Signal{
	"oom-killed": {{Kind: triage.SigPodTerminated, Text: "OOMKilled exit 137 (taskmanager)"}},
	"taskmanager-crashloop": {
		{Kind: triage.SigPodWaiting, Text: "CrashLoopBackOff: back-off 40s restarting failed container (taskmanager)"},
		{Kind: triage.SigPodTerminated, Text: "Error exit 1 (taskmanager)"},
	},
	"quota-exceeded":            {{Kind: triage.SigEvent, Text: `FailedCreate: pods "x" is forbidden: exceeded quota: team-a, requested: cpu=2, used: cpu=8, limited: cpu=8`}},
	"admission-denied":          {{Kind: triage.SigEvent, Text: `FailedCreate: admission webhook "policy.example.com" denied the request: privileged containers are not allowed`}},
	"validation-error":          {{Kind: triage.SigEvent, Text: "ValidationError: Job parallelism must be larger than 0"}},
	"unsupported-flink-version": {{Kind: triage.SigEvent, Text: "UnsupportedFlinkVersion: Flink version v1_15 is not supported by this operator"}},
	"taskmanagers-short":        {{Kind: triage.SigHealth, Text: "1/3 TaskManagers ready"}},
	"backpressure": {
		{Kind: triage.SigBackpressure, Text: "Source: orders: backpressured 870 ms/s"},
		{Kind: triage.SigBottleneck, Text: "Enrich -> Sink: payments: busy 990 ms/s"},
	},
	"sink-idle":     {{Kind: triage.SigSinkIdle, Text: "Sink: payments receives nothing while the sources emit 213 records/s"}},
	"ha-leftover":   {{Kind: triage.SigHALeftover, Text: "job plan of 0000, not the current job 1111"}},
	"multiple-jobs": {{Kind: triage.SigJobsMultiple, Text: "2 jobs run on the application cluster: 0000, 1111"}},
	"checkpoints-stale": {
		{Kind: triage.SigCheckpointStale, Text: "last completed checkpoint is 45m old, interval 60s"},
		{Kind: triage.SigCheckpointFail, Text: "Checkpoint expired before completing."},
	},
}

func fire(t *testing.T, signals []triage.Signal) []string {
	t.Helper()
	findings, err := triage.Evaluate(triage.DefaultRules(), signals, job)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(findings))
	for _, f := range findings {
		out = append(out, f.ID)
	}
	return out
}

// TestEveryRuleFires guards the rule set: a default rule ships with a fixture that triggers it, or with a
// synthetic case here, and every synthetic case triggers its rule.
func TestEveryRuleFires(t *testing.T) {
	fired := map[string]bool{}
	dirs, _ := filepath.Glob("../../testdata/fixtures/scenarios-*/*")
	for _, dir := range dirs {
		for _, id := range fire(t, triage.Signals(scenario(t, dir))) {
			fired[id] = true
		}
	}
	for id, signals := range synthetic {
		if got := fire(t, signals); !slices.Contains(got, id) {
			t.Errorf("synthetic case for %s fires %v", id, got)
		}
		if fired[id] {
			t.Errorf("%s fires on a fixture; drop its synthetic case", id)
		}
	}
	for _, r := range triage.DefaultRules() {
		if !fired[r.ID] && synthetic[r.ID] == nil {
			t.Errorf("rule %s fires on no fixture and has no synthetic case", r.ID)
		}
	}
}

// A crashing TaskManager is not reported as the JobManager crashing.
func TestCrashLoopComponent(t *testing.T) {
	if got := fire(t, synthetic["taskmanager-crashloop"]); slices.Contains(got, "jobmanager-crashloop") {
		t.Errorf("TaskManager crash fires %v", got)
	}
	jm := []triage.Signal{{Kind: triage.SigPodWaiting, Text: "CrashLoopBackOff: back-off (jobmanager)"}}
	if got := fire(t, jm); !slices.Equal(got, []string{"jobmanager-crashloop"}) {
		t.Errorf("JobManager crash fires %v", got)
	}
}

func TestDefaultSupersedesKnownRules(t *testing.T) {
	ids := map[string]bool{}
	for _, r := range triage.DefaultRules() {
		ids[r.ID] = true
	}
	for _, r := range triage.DefaultRules() {
		for _, id := range r.Supersedes {
			if !ids[id] {
				t.Errorf("%s supersedes unknown %s", r.ID, id)
			}
		}
	}
}

func TestRuleTemplates(t *testing.T) {
	findings, err := triage.Evaluate(triage.DefaultRules(), synthetic["checkpoints-stale"], job)
	if err != nil || len(findings) != 1 {
		t.Fatalf("%v %v", findings, err)
	}
	f := findings[0]
	if f.Title != "Checkpoints are not completing: last completed checkpoint is 45m old, interval 60s" ||
		!strings.Contains(f.Explain, "The latest failure: Checkpoint expired before completing.") ||
		f.Remedy != "kubectl flink checkpoints team-a/orders" || len(f.Evidence) != 2 {
		t.Errorf("%+v", f)
	}
	// Without the optional failure, the sentence about it is left out.
	findings, _ = triage.Evaluate(triage.DefaultRules(), synthetic["checkpoints-stale"][:1], job)
	if strings.Contains(findings[0].Explain, "latest failure") {
		t.Errorf("%q", findings[0].Explain)
	}
}

func TestParseRulesRejects(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, want string }{
		"unknown signal": {`rules: [{id: a, layer: runtime, severity: FAILED, title: t, explain: e, when: [{signal: nope, match: x}]}]`, `unknown signal "nope"`},
		"bad regex":      {`rules: [{id: a, layer: runtime, severity: FAILED, title: t, explain: e, when: [{signal: event, match: "("}]}]`, "missing closing )"},
		"unknown key":    {`rules: [{id: a, layer: runtime, severity: FAILED, title: "{{.nope}}", explain: e, when: [{signal: event, match: x}]}]`, `map has no entry for key "nope"`},
		"no condition":   {`rules: [{id: a, layer: runtime, severity: FAILED, title: t, explain: e}]`, "needs a when or any"},
		"layer":          {`rules: [{id: a, layer: network, severity: FAILED, title: t, explain: e, when: [{signal: event, match: x}]}]`, `unknown layer "network"`},
		"severity":       {`rules: [{id: a, layer: runtime, severity: BAD, title: t, explain: e, when: [{signal: event, match: x}]}]`, `unknown health "BAD"`},
		"duplicate": {`rules: [{id: a, layer: runtime, severity: OK, title: t, explain: e, when: [{signal: event, match: x}]},
			{id: a, layer: runtime, severity: OK, title: t, explain: e, when: [{signal: event, match: y}]}]`, "id used twice"},
		"unknown field": {`rules: [{id: a, colour: red}]`, "unknown field"},
	} {
		if _, err := triage.ParseRules([]byte(tc.yaml)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", name, err, tc.want)
		}
	}
	// A capture is usable in templates once a condition defines it.
	if _, err := triage.ParseRules([]byte(`rules: [{id: a, layer: runtime, severity: OK, title: "{{.x}}", explain: e, when: [{signal: event, match: "(?P<x>.)"}]}]`)); err != nil {
		t.Error(err)
	}
}

func kinds(signals []triage.Signal) string {
	out := make([]string, 0, len(signals))
	for _, s := range signals {
		out = append(out, s.Kind)
	}
	return strings.Join(out, ",")
}

func TestJudge(t *testing.T) {
	ev := &triage.Evidence{Job: job, Health: model.Assessment{Health: model.Failed}}
	// image-pull supersedes the crash loop it causes; nothing else strong was seen.
	signals := []triage.Signal{
		{Kind: triage.SigPodWaiting, Text: `ImagePullBackOff: Back-off pulling image "flink:nope"`},
		{Kind: triage.SigEvent, Text: "CrashLoopBackOff: back-off 10s restarting failed container"},
		{Kind: triage.SigPodTerminated, Text: "OOMKilled exit 137 (taskmanager)"},
	}
	v, err := triage.Judge(ev, signals, triage.DefaultRules())
	if err != nil || ids(v) != "image-pull,oom-killed" || !v.Explained || len(v.Unexplained) != 0 {
		t.Errorf("superseded: %s %v %+v", ids(v), err, v.Unexplained)
	}
	// Nothing known: the strongest signals, in order of preference.
	signals = []triage.Signal{
		{Kind: triage.SigEvent, Text: "Unhealthy: Readiness probe failed"},
		{Kind: triage.SigExceptionCause, Text: "java.lang.IllegalStateException: Unexpected record"},
		{Kind: triage.SigLog, Text: "ERROR something"},
	}
	v, _ = triage.Judge(ev, signals, triage.DefaultRules())
	if len(v.Findings) != 0 || v.Explained || kinds(v.Unexplained) != "exception.cause,event,log.jobmanager" {
		t.Errorf("unknown: %+v", v)
	}
	// A match does not hide a second problem: the unrelated exception is still listed.
	signals = []triage.Signal{
		{Kind: triage.SigPodWaiting, Text: `ImagePullBackOff: Back-off pulling image "flink:nope" (taskmanager)`},
		{Kind: triage.SigExceptionCause, Text: "java.lang.IllegalStateException: Unexpected record"},
		{Kind: triage.SigEvent, Text: `Failed: Failed to pull image "flink:nope": not found`},
	}
	v, _ = triage.Judge(ev, signals, triage.DefaultRules())
	if ids(v) != "image-pull" || kinds(v.Unexplained) != "exception.cause" {
		t.Errorf("second problem: %s %+v", ids(v), v.Unexplained)
	}
	// A note that does not explain the health leaves the health itself to look at.
	v, _ = triage.Judge(ev, []triage.Signal{
		{Kind: triage.SigHealth, Text: "JobManager DEPLOYING for 20m"},
		{Kind: triage.SigException, Text: "org.apache.flink.util.FlinkExpectedException: The TaskExecutor is shutting down."},
	}, triage.DefaultRules())
	if ids(v) != "taskmanager-lost" || v.Explained || kinds(v.Unexplained) != "health.finding" {
		t.Errorf("note: %s %+v", ids(v), v.Unexplained)
	}
	// A healthy job with nothing found gets nothing more; one with an unexplained exception gets it, but not
	// events or log lines, which healthy jobs have too.
	ev.Health.Health = model.OK
	if v, _ = triage.Judge(ev, []triage.Signal{{Kind: triage.SigEvent, Text: "Unhealthy: probe"}}, triage.DefaultRules()); v.Unexplained != nil {
		t.Errorf("healthy: %+v", v.Unexplained)
	}
	v, _ = triage.Judge(ev, []triage.Signal{
		{Kind: triage.SigException, Text: "org.apache.flink.util.FlinkExpectedException: The TaskExecutor is shutting down."},
		{Kind: triage.SigExceptionCause, Text: "java.io.IOException: disk full"},
		{Kind: triage.SigEvent, Text: "Unhealthy: probe"},
	}, triage.DefaultRules())
	if kinds(v.Unexplained) != "exception.cause" {
		t.Errorf("healthy with a note: %+v", v.Unexplained)
	}
}

// A pod's failure needs the pod in that state now: an event of a startup hiccup that resolved itself, such as the
// operator's config ConfigMap not being there yet, is not a finding. Seen in CI on operator 1.16.1.
func TestEventsAloneAreNotCurrent(t *testing.T) {
	mount := triage.Signal{Kind: triage.SigEvent, Text: `FailedMount: MountVolume.SetUp failed for volume "flink-config-volume" : configmap "flink-config-x" not found`}
	pull := triage.Signal{Kind: triage.SigEvent, Text: `Failed: Failed to pull image "flink:2.2": rpc error: code = Unavailable`}
	sched := triage.Signal{Kind: triage.SigEvent, Text: "FailedScheduling: 0/1 nodes are available: 1 Insufficient cpu."}
	crash := triage.Signal{Kind: triage.SigEvent, Text: "CrashLoopBackOff: [flink-main-container] back-off 10s restarting failed container"}
	if got := fire(t, []triage.Signal{mount, pull, sched, crash}); len(got) != 0 {
		t.Errorf("resolved hiccups fire %v", got)
	}
	creating := triage.Signal{Kind: triage.SigPodCreating, Text: "ContainerCreating (jobmanager)"}
	if got := fire(t, []triage.Signal{mount, creating}); !slices.Equal(got, []string{"mount-failed"}) {
		t.Errorf("stuck mount fires %v", got)
	}
}
