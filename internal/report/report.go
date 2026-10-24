// Package report builds the debugging handover for one job: a bundle with everything the cluster and the
// JobManager know about it, secrets redacted, and a summary short enough to paste into a chat.
package report

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/redact"
)

// File is one file of the bundle, already redacted.
type File struct {
	Name     string `json:"name"`
	Data     []byte `json:"-"`
	Redacted int    `json:"redacted"`
}

// Bundle is the handover for one job.
type Bundle struct {
	Files []File
	// Gaps say what could not be collected and why.
	Gaps []string
}

// Options shape the bundle; the defaults keep it small enough to share in a chat.
type Options struct {
	Tail            int64 // log lines per container
	MaxTaskManagers int   // TaskManager pods whose logs are included
}

// Defaults for Options.
const (
	DefaultTail            = 2000
	DefaultMaxTaskManagers = 5
	// Timeout bounds the collection; logs and REST of a large job take longer than why's evidence.
	Timeout = 60 * time.Second
)

// Source is where the raw artifacts come from; REST is nil when the JobManager cannot be asked.
type Source struct {
	Kube *kube.Client
	REST flink.Access
}

type collector struct {
	r      *redact.Redactor
	mu     sync.Mutex
	bundle *Bundle
}

func (c *collector) gap(what string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bundle.Gaps = append(c.bundle.Gaps, what+": "+err.Error())
}

// addJSON redacts a JSON document and adds it. A document that does not parse is added as text, redacted. The
// redactor and its count are shared, so redacting and counting happen under one lock.
func (c *collector) addJSON(name string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out, err := c.r.JSON(b)
	if err != nil {
		c.r.Count()
		out = []byte(c.r.Text(string(b)))
	}
	c.bundle.Files = append(c.bundle.Files, File{Name: name, Data: out, Redacted: c.r.Count()})
}

func (c *collector) addText(name, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.r.Text(text)
	c.bundle.Files = append(c.bundle.Files, File{Name: name, Data: []byte(out), Redacted: c.r.Count()})
}

// Add puts an already rendered file, such as why's output, into the bundle after redacting it as text.
func (b *Bundle) Add(r *redact.Redactor, name, text string) {
	out := r.Text(text)
	b.Files = append(b.Files, File{Name: name, Data: []byte(out), Redacted: r.Count()})
}

// Collect gathers the raw artifacts of job; rawCR is the resource as read for history, so that both show the
// same version. Nothing fails it: what cannot be read becomes a gap.
func Collect(ctx context.Context, src Source, job *model.Job, rawCR []byte, pods []kube.Pod, events []kube.Event, r *redact.Redactor, opt Options) *Bundle {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	c := &collector{r: r, bundle: &Bundle{}}
	cluster := job.Name
	if job.Kind == model.KindSessionJob {
		cluster = job.DeploymentName
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		if len(rawCR) > 0 {
			c.addJSON("resource.json", dropLastApplied(rawCR))
		}
		if job.Kind == model.KindSessionJob {
			if raw, err := src.Kube.GetRaw(ctx, kube.Deployments, job.Namespace, cluster); err != nil {
				c.gap("session cluster", err)
			} else {
				c.addJSON("session-cluster.json", dropLastApplied(raw))
			}
		}
	})
	wg.Go(func() {
		raw, err := src.Kube.ListRaw(ctx, kube.Pods, job.Namespace, kube.FlinkPodSelector+","+kube.LabelApp+"="+cluster, "")
		if err != nil {
			c.gap("pods", err)
			return
		}
		c.addJSON("pods.json", raw)
	})
	wg.Go(func() {
		b, err := json.Marshal(map[string]any{"items": events})
		if err == nil {
			c.addJSON("events.json", b)
		}
	})
	wg.Go(func() {
		// The configuration the operator renders for the cluster, which is what Flink reads.
		raw, err := src.Kube.GetRaw(ctx, kube.ConfigMaps, job.Namespace, "flink-config-"+cluster)
		if err != nil {
			c.gap("Flink configuration ConfigMap", err)
			return
		}
		c.addJSON("flink-config.json", raw)
	})
	wg.Go(func() { collectLogs(ctx, c, src.Kube, job.Namespace, pods, opt) })
	wg.Go(func() { collectREST(ctx, c, src.REST, job, cluster) })
	wg.Wait()
	slices.SortFunc(c.bundle.Files, func(a, b File) int { return strings.Compare(a.Name, b.Name) })
	slices.Sort(c.bundle.Gaps)
	return c.bundle
}

// dropLastApplied removes kubectl's last-applied-configuration annotation: a copy of the whole spec as one
// JSON string, which would only repeat the spec.
func dropLastApplied(raw []byte) []byte {
	var obj map[string]any
	if json.Unmarshal(raw, &obj) != nil {
		return raw
	}
	if md, ok := obj["metadata"].(map[string]any); ok {
		if ann, ok := md["annotations"].(map[string]any); ok {
			delete(ann, "kubectl.kubernetes.io/last-applied-configuration")
		}
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

// collectLogs reads every JobManager container's log and, of the TaskManagers, those with trouble first, up to
// MaxTaskManagers; each current log and, after a restart, the previous one.
func collectLogs(ctx context.Context, c *collector, kc *kube.Client, namespace string, pods []kube.Pod, opt Options) {
	var jms, tms []*kube.Pod
	for i := range pods {
		switch pods[i].Labels[kube.LabelComponent] {
		case kube.ComponentJM:
			jms = append(jms, &pods[i])
		case kube.ComponentTM:
			tms = append(tms, &pods[i])
		}
	}
	trouble := func(p *kube.Pod) bool { return !p.Ready() || restarts(p) > 0 }
	slices.SortStableFunc(tms, func(a, b *kube.Pod) int {
		switch ta, tb := trouble(a), trouble(b); {
		case ta && !tb:
			return -1
		case tb && !ta:
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	if len(tms) > opt.MaxTaskManagers {
		c.gap("TaskManager logs", fmt.Errorf("%d of %d included; raise --max-taskmanagers for more", opt.MaxTaskManagers, len(tms)))
		tms = tms[:opt.MaxTaskManagers]
	}
	// In parallel, a few at a time, within the report's timeout.
	sem := make(chan struct{}, parallelLogs)
	var wg sync.WaitGroup
	for _, p := range append(jms, tms...) {
		for _, previous := range []bool{false, true} {
			if previous && restarts(p) == 0 {
				continue
			}
			wg.Go(func() {
				sem <- struct{}{}
				defer func() { <-sem }()
				readLog(ctx, c, kc, namespace, p, previous, opt.Tail)
			})
		}
	}
	wg.Wait()
}

// parallelLogs bounds the log streams in flight.
const parallelLogs = 4

func readLog(ctx context.Context, c *collector, kc *kube.Client, namespace string, p *kube.Pod, previous bool, tail int64) {
	rc, err := kc.Logs(ctx, namespace, p.Name, kube.LogOptions{Container: kube.MainContainer, Previous: previous, TailLines: tail})
	if err != nil {
		if !previous || !strings.Contains(err.Error(), "not found") {
			c.gap("log of "+p.Name, err)
		}
		return
	}
	b, err := io.ReadAll(io.LimitReader(rc, 32<<20))
	_ = rc.Close()
	if err != nil {
		c.gap("log of "+p.Name, err)
		return
	}
	name := "logs/" + p.Name + ".txt"
	if previous {
		name = "logs/" + p.Name + ".previous.txt"
	}
	c.addText(name, string(b))
}

func restarts(p *kube.Pod) int32 {
	var n int32
	for _, cs := range p.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}

var errNotAsked = errors.New("not asked")

// collectREST saves the JobManager's answers as it sends them, redacted.
func collectREST(ctx context.Context, c *collector, access flink.Access, job *model.Job, cluster string) {
	switch {
	case access == nil:
		c.gap("JobManager REST API", fmt.Errorf("%w: no access to the JobManager", errNotAsked))
		return
	case job.Kind == model.KindDeployment && job.JMStatus != "READY":
		c.gap("JobManager REST API", fmt.Errorf("%w: the JobManager is %s", errNotAsked, cmp.Or(job.JMStatus, "UNKNOWN")))
		return
	}
	jid := url.PathEscape(job.JobID)
	paths := map[string]string{"config": "/config", "jobmanager-config": "/jobmanager/config", "jobs-overview": "/jobs/overview", "taskmanagers": "/taskmanagers"}
	if job.JobID != "" {
		paths["job"] = "/jobs/" + jid
		paths["exceptions"] = "/jobs/" + jid + "/exceptions?maxExceptions=50"
		paths["checkpoints"] = "/jobs/" + jid + "/checkpoints"
		paths["checkpoints-config"] = "/jobs/" + jid + "/checkpoints/config"
		paths["job-config"] = "/jobs/" + jid + "/config"
	}
	err := access.Do(ctx, job.Namespace, cluster, func(fc *flink.Client) error {
		names := make([]string, 0, len(paths))
		for n := range paths {
			names = append(names, n)
		}
		slices.Sort(names)
		for _, name := range names {
			raw, err := fc.GetRaw(ctx, paths[name])
			switch {
			case flink.IsNotFound(err) && name == "checkpoints-config":
				// no checkpointing
			case err != nil:
				c.gap("REST "+paths[name], err)
			default:
				c.addJSON("rest/"+name+".json", raw)
			}
		}
		return nil
	})
	if err != nil {
		c.gap("JobManager REST API", err)
	}
}
