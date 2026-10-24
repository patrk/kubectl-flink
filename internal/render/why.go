package render

import (
	"fmt"
	"io"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// WhyReport is the -o json and -o yaml document of why.
type WhyReport struct {
	Job          JobRef           `json:"job"`
	Health       model.Assessment `json:"health"`
	Verdict      *triage.Verdict  `json:"verdict"`
	Signals      []triage.Signal  `json:"signals"`
	NotCollected []triage.Gap     `json:"notCollected,omitempty"`
}

// NewWhyReport builds the machine-readable report.
func NewWhyReport(ev *triage.Evidence, v *triage.Verdict, signals []triage.Signal) WhyReport {
	return WhyReport{Job: NewJobRef(ev.Job), Health: ev.Health, Verdict: v, Signals: signals, NotCollected: ev.Gaps}
}

// Limits keep why on about one screen; the building-block commands show everything.
const (
	whyWidth  = 160
	whyEvents = 5
	whyLogs   = 8
)

// Why prints the verdict, then the evidence by layer.
func Why(out io.Writer, ev *triage.Evidence, v *triage.Verdict) error {
	w := &lineWriter{w: out}
	j := ev.Job
	w.line(0, "%s · %s", NewJobRef(j), ev.Health.Health)
	for i, f := range ev.Health.Findings {
		if i == 2 {
			break
		}
		w.line(1, "%s", f.Text(ev.Now))
	}

	w.blank()
	w.line(0, "Verdict")
	if len(v.Findings) > 0 {
		for i := range v.Findings {
			f := &v.Findings[i]
			w.line(1, "%d. %s: %s (%s)", i+1, f.Severity, f.Title, f.Layer)
			w.wrap(2, f.Explain)
			if f.Remedy != "" {
				w.wrap(2, "Next: "+f.Remedy)
			}
			for _, s := range f.Evidence {
				w.line(2, "· %s: %s", s.Kind, s.Text)
			}
		}
	}
	switch {
	case len(v.Leads) > 0 && len(v.Findings) > 0:
		w.line(1, "None of these explains the %s health. Look first at:", v.Health)
	case len(v.Leads) > 0:
		w.line(1, "No known pattern matches. Look first at:")
	case len(v.Findings) == 0:
		w.line(1, "Nothing wrong found.")
	}
	for _, s := range v.Leads {
		w.line(2, "· %s: %s", s.Kind, s.Text)
	}
	for _, layer := range triage.Layers {
		w.blank()
		w.line(0, "%s", strings.ToUpper(string(layer[:1]))+string(layer[1:]))
		switch layer {
		case triage.Operator:
			operatorEvidence(w, ev)
		case triage.Kubernetes:
			kubernetesEvidence(w, ev)
		case triage.Runtime:
			runtimeEvidence(w, ev)
		}
		for _, g := range ev.Gaps {
			if g.Layer == layer {
				w.line(1, "not collected: %s: %s", g.What, g.Why)
			}
		}
	}
	return w.err
}

func operatorEvidence(w *lineWriter, ev *triage.Evidence) {
	j := ev.Job
	w.line(1, "lifecycle %s, JobManager %s, job %s, reconciled %s, generation %d (observed %d)",
		dash(j.Lifecycle), dash(j.JMStatus), dash(j.JobState), age(j.ReconciledAt, ev.Now), j.Generation, j.ObservedGeneration)
	if e := j.Error; e != nil {
		w.line(1, "error: %s", e.Summary(whyWidth))
		for _, c := range e.Causes {
			w.line(2, "caused by %s: %s", shortType(c.Type), flink.FirstLine(c.Message))
		}
	}
	if len(j.SpecOwners) > 0 {
		w.line(1, "spec written by %s, last %s", strings.Join(j.SpecOwners, ", "), age(j.SpecChangedAt, ev.Now))
	}
}

func kubernetesEvidence(w *lineWriter, ev *triage.Evidence) {
	if len(ev.Pods) == 0 && !hasGap(ev, triage.Kubernetes, "pods") {
		w.line(1, "no Flink pods")
	}
	for i := range ev.Pods {
		p := &ev.Pods[i]
		w.line(1, "pod %s (%s): %s", p.Name, p.Labels[kube.LabelComponent], podState(p, ev.Now))
	}
	n := 0
	for i := range ev.Events {
		e := &ev.Events[i]
		if e.Type != "Warning" {
			continue
		}
		if n == 0 {
			w.line(1, "warning events, newest first:")
		}
		n++
		if n > whyEvents {
			break
		}
		times := ""
		if c := e.Times(); c > 1 {
			times = fmt.Sprintf(" ×%d", c)
		}
		w.line(2, "%s %s%s on %s: %s", age(e.Last(), ev.Now), e.Reason, times, e.InvolvedObject.Kind, e.Message)
	}
	if len(ev.Logs) > 0 {
		w.line(1, "JobManager log, error lines:")
		for i, l := range ev.Logs {
			if i == whyLogs {
				w.line(2, "… %d more: kubectl flink logs %s%s", len(ev.Logs)-whyLogs, ev.Job.Key(), previousFlag(l))
				break
			}
			w.line(2, "%s", l.Text)
		}
	}
}

func previousFlag(l triage.LogLine) string {
	if l.Previous {
		return " --previous"
	}
	return ""
}

func runtimeEvidence(w *lineWriter, ev *triage.Evidence) {
	r := ev.REST
	if r == nil || r.Job == nil {
		return
	}
	w.line(1, "job %s, Flink %s, %d job(s) on the cluster", r.Job.State, dash(r.FlinkVersion), len(r.Jobs))
	if n := len(r.Exceptions); n > 0 {
		e := &r.Exceptions[0]
		w.line(1, "%d failure(s), newest %s: %s", n, age(e.Timestamp.Time(), ev.Now), flink.FirstLine(e.Message()))
		if c := e.Cause(); c != "" {
			w.line(2, "caused by %s", c)
		}
	} else {
		w.line(1, "no failures since the job started")
	}
	if cps := r.Checkpoints; cps != nil {
		line := "checkpoints: "
		if c := cps.Latest.Completed; c != nil {
			line += fmt.Sprintf("last completed #%d %s", c.ID, age(c.LatestAck.Time(), ev.Now))
		} else {
			line += "none completed"
		}
		switch cfg := r.CheckpointConfig; {
		case cfg == nil || cfg.Interval == triage.PeriodicOff:
			line += ", periodic checkpoints off"
		default:
			line += ", interval " + triage.Short(time.Duration(cfg.Interval)*time.Millisecond)
		}
		line += fmt.Sprintf(", %d completed, %d failed", cps.Counts.Completed, cps.Counts.Failed)
		w.line(1, "%s", line)
		if rs := cps.Latest.Restored; rs != nil {
			w.line(2, "restored from #%d %s", rs.ID, age(rs.RestoredAt.Time(), ev.Now))
		}
	}
}

func podState(p *kube.Pod, now time.Time) string {
	parts := []string{p.Status.Phase}
	for _, c := range p.Status.Conditions {
		if c.Type == "PodScheduled" && c.Status == "False" {
			parts = append(parts, "unschedulable")
		}
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Name != kube.MainContainer {
			continue
		}
		if w := cs.State.Waiting; w != nil {
			parts = append(parts, "waiting "+w.Reason)
		}
		switch cs.RestartCount {
		case 0:
		case 1:
			parts = append(parts, "1 restart")
		default:
			parts = append(parts, fmt.Sprintf("%d restarts", cs.RestartCount))
		}
		if t := cs.LastState.Terminated; t != nil {
			parts = append(parts, fmt.Sprintf("last %s exit %d %s", t.Reason, t.ExitCode, age(t.FinishedAt.Time, now)))
		}
		if cs.Ready {
			parts = append(parts, "ready")
		}
	}
	return strings.Join(parts, ", ")
}

func hasGap(ev *triage.Evidence, layer triage.Layer, what string) bool {
	for _, g := range ev.Gaps {
		if g.Layer == layer && g.What == what {
			return true
		}
	}
	return false
}

func age(t, now time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return duration.HumanDuration(now.Sub(t)) + " ago"
}

func shortType(t string) string {
	if i := strings.LastIndex(t, "."); i >= 0 {
		return t[i+1:]
	}
	return t
}

// lineWriter writes indented lines cut to whyWidth and keeps the first error.
type lineWriter struct {
	w   io.Writer
	err error
}

func (l *lineWriter) line(indent int, format string, args ...any) {
	if l.err != nil {
		return
	}
	s := strings.ReplaceAll(fmt.Sprintf(format, args...), "\n", " ")
	lead := len(s) - len(strings.TrimLeft(s, " "))
	s = truncate(strings.Repeat("  ", indent)+s[:lead]+strings.Join(strings.Fields(s), " "), whyWidth)
	_, l.err = fmt.Fprintln(l.w, s)
}

// wrap writes prose over as many lines as it needs, cutting at spaces.
func (l *lineWriter) wrap(indent int, text string) {
	prefix := strings.Repeat("  ", indent)
	line := prefix
	for _, word := range strings.Fields(text) {
		if len(line) > len(prefix) && len(line)+1+len(word) > whyWidth {
			l.line(0, "%s", line)
			line = prefix
		}
		if len(line) > len(prefix) {
			line += " "
		}
		line += word
	}
	if len(line) > len(prefix) {
		l.line(0, "%s", line)
	}
}

func (l *lineWriter) blank() {
	if l.err == nil {
		_, l.err = fmt.Fprintln(l.w)
	}
}
