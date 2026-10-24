// Package triage explains why a job is unhealthy: it collects evidence from the operator, Kubernetes and the
// JobManager, reduces it to signals, and matches the signals against rules to give a verdict. The evidence is
// the product; rules only name the patterns they know, and a job no rule matches still gets its evidence.
package triage

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
)

// Layer is where a piece of evidence or a finding comes from, in the order why prints them.
type Layer string

// Layers.
const (
	Operator   Layer = "operator"
	Kubernetes Layer = "kubernetes"
	Runtime    Layer = "runtime"
)

// Layers in print order.
var Layers = []Layer{Operator, Kubernetes, Runtime}

// Gap is evidence that could not be collected, and why; it is shown, never hidden.
type Gap struct {
	Layer Layer  `json:"layer"`
	What  string `json:"what"`
	Why   string `json:"why"`
}

// LogLine is one line of a JobManager log that looks like a problem: an error, an exception or its cause.
type LogLine struct {
	Pod      string `json:"pod"`
	Previous bool   `json:"previous,omitempty"`
	Text     string `json:"text"`
}

// REST is what the JobManager reported.
type REST struct {
	FlinkVersion     string                  `json:"flinkVersion,omitempty"`
	Jobs             []flink.JobOverview     `json:"jobs,omitempty"`
	Job              *flink.Job              `json:"job,omitempty"`
	Exceptions       []flink.Exception       `json:"exceptions,omitempty"`
	Checkpoints      *flink.Checkpoints      `json:"checkpoints,omitempty"`
	CheckpointConfig *flink.CheckpointConfig `json:"checkpointConfig,omitempty"`
}

// Evidence is everything collected about one job.
type Evidence struct {
	Job    *model.Job       `json:"-"`
	Health model.Assessment `json:"health"`
	Pods   []kube.Pod       `json:"-"`
	Events []kube.Event     `json:"-"`
	Logs   []LogLine        `json:"logLines,omitempty"`
	REST   *REST            `json:"rest,omitempty"`
	Gaps   []Gap            `json:"notCollected,omitempty"`
	Now    time.Time        `json:"-"`
}

// Timeout bounds the whole collection, so that why answers even when part of the cluster does not.
const Timeout = 20 * time.Second

// logTail is how many lines of each JobManager log are read.
const logTail = 300

// problemLine selects the log lines kept as evidence.
var problemLine = regexp.MustCompile(`\b(ERROR|FATAL)\b|Exception|^Caused by:|\bError:|OutOfMemory`)

// Source is where Collect reads from. REST is nil when the JobManager is not to be asked. ForceREST asks it
// whatever the resource says about the JobManager, for a REST URL the user gave.
type Source struct {
	Kube      *kube.Client
	REST      flink.Access
	ForceREST bool
}

// Collect gathers the evidence for job at now. It never fails: what cannot be collected becomes a Gap.
func Collect(ctx context.Context, src Source, job *model.Job, now time.Time, th model.Thresholds) *Evidence {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	ev := &Evidence{Job: job, Now: now}
	var mu sync.Mutex
	gap := func(layer Layer, what string, err error) {
		mu.Lock()
		defer mu.Unlock()
		ev.Gaps = append(ev.Gaps, Gap{Layer: layer, What: what, Why: err.Error()})
	}

	cluster := job.Name
	if job.Kind == model.KindSessionJob {
		cluster = job.DeploymentName
	}
	pods, err := src.Kube.List(ctx, kube.Pods, job.Namespace, kube.FlinkPodSelector+","+kube.LabelApp+"="+cluster)
	if err != nil {
		gap(Kubernetes, "pods", err)
	} else {
		ev.Pods = pods.(*kube.PodList).Items
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		uids := make([]types.UID, 0, 1+len(ev.Pods))
		uids = append(uids, types.UID(job.UID))
		for i := range ev.Pods {
			uids = append(uids, ev.Pods[i].UID)
		}
		events, err := src.Kube.EventsFor(ctx, job.Namespace, uids)
		if err != nil {
			gap(Kubernetes, "events", err)
		}
		ev.Events = events
	})
	wg.Go(func() {
		lines, errs := collectLogs(ctx, src.Kube, job.Namespace, ev.Pods)
		ev.Logs = lines
		for _, err := range errs {
			gap(Kubernetes, "JobManager log", err)
		}
	})
	wg.Go(func() {
		r, err := collectREST(ctx, src.REST, src.ForceREST, job, cluster)
		if err != nil {
			gap(Runtime, "JobManager REST API", err)
		}
		ev.REST = r
	})
	wg.Wait()

	// Health with the pods just read, the same derivation as ls.
	jobs := []model.Job{*job}
	model.AttachPods(jobs, model.SummarisePods(ev.Pods), func(string) bool { return pods != nil })
	ev.Job = &jobs[0]
	ev.Health = model.Assess(ev.Job, now, th)
	return ev
}

// collectLogs reads the problem lines of every JobManager container's log, and of its previous container when
// it has restarted, which is where a crash at startup explains itself.
func collectLogs(ctx context.Context, kc *kube.Client, namespace string, pods []kube.Pod) (lines []LogLine, errs []error) {
	for i := range pods {
		p := &pods[i]
		if p.Labels[kube.LabelComponent] != kube.ComponentJM {
			continue
		}
		var restarted, running bool
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Name == kube.MainContainer {
				restarted = cs.RestartCount > 0
				running = cs.State.Running != nil
			}
		}
		read := func(previous bool) {
			rc, err := kc.Logs(ctx, namespace, p.Name, kube.LogOptions{Container: kube.MainContainer, Previous: previous, TailLines: logTail})
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", p.Name, err))
				return
			}
			defer rc.Close()
			sc := bufio.NewScanner(rc)
			sc.Buffer(make([]byte, 64<<10), 1<<20)
			for sc.Scan() {
				if t := strings.TrimSpace(sc.Text()); problemLine.MatchString(t) {
					lines = append(lines, LogLine{Pod: p.Name, Previous: previous, Text: t})
				}
			}
		}
		if restarted {
			read(true)
		}
		if running {
			read(false)
		}
	}
	return lines, errs
}

// errNotAsked explains why REST was not called; it is shown as a gap.
var errNotAsked = errors.New("not asked")

func collectREST(ctx context.Context, access flink.Access, force bool, job *model.Job, cluster string) (*REST, error) {
	switch {
	case access == nil:
		return nil, fmt.Errorf("%w: no access to the JobManager", errNotAsked)
	case !force && job.Kind == model.KindDeployment && job.JMStatus != "READY":
		return nil, fmt.Errorf("%w: the JobManager is %s", errNotAsked, cmp.Or(job.JMStatus, "UNKNOWN"))
	case job.JobID == "":
		return nil, fmt.Errorf("%w: the job has no ID in its status", errNotAsked)
	}
	r := &REST{}
	err := access.Do(ctx, job.Namespace, cluster, func(c *flink.Client) error {
		cfg, err := c.Config(ctx)
		if err != nil {
			return err
		}
		r.FlinkVersion = cfg.FlinkVersion
		if r.Jobs, err = c.Jobs(ctx); err != nil {
			return err
		}
		if r.Job, err = c.Job(ctx, job.JobID); err != nil {
			return err
		}
		ex, err := c.Exceptions(ctx, job.JobID, 10)
		if err != nil {
			return err
		}
		r.Exceptions = ex.History.Entries
		if r.Checkpoints, err = c.Checkpoints(ctx, job.JobID); err != nil {
			return err
		}
		if r.CheckpointConfig, err = c.CheckpointConfig(ctx, job.JobID); flink.IsNotFound(err) {
			r.CheckpointConfig, err = nil, nil
		}
		return err
	})
	if err != nil {
		return r, err
	}
	return r, nil
}
