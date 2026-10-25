package render

import (
	"cmp"
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
	return WhyWidth(out, ev, v, whyWidth)
}

// WhyWidth is Why with lines cut and wrapped to width, for a terminal pane.
func WhyWidth(out io.Writer, ev *triage.Evidence, v *triage.Verdict, width int) error {
	w := &lineWriter{w: out, width: width}
	j := ev.Job
	w.line(0, "%s · %s", NewJobRef(j), ev.Health.Health)
	// Health comes from the status and pods, as ls shows it; a pattern may see worse, such as back pressure in
	// the metrics, and the header must not read as all clear above it.
	if len(v.Findings) > 0 && v.Findings[0].Severity > ev.Health.Health {
		f := &v.Findings[0]
		w.line(1, "%s from the status and pods, as ls shows it; the patterns below find %s: %s", ev.Health.Health, f.Severity, f.Title)
	}
	for i, f := range ev.Health.Findings {
		if i == 2 {
			break
		}
		w.line(1, "%s", f.Text(ev.Now))
	}

	w.blank()
	w.line(0, "Matched patterns")
	if len(v.Findings) == 0 {
		w.line(1, "none of the known patterns matches")
	}
	if len(v.Findings) > 0 {
		for i := range v.Findings {
			f := &v.Findings[i]
			when := ""
			if t := f.Newest(); !t.IsZero() {
				when = ", newest evidence " + age(t, ev.Now)
			}
			if f.Past {
				when += ", before the job's current run, which began " + age(triage.RunningSince(ev), ev.Now)
			}
			w.line(1, "%d. %s: %s (%s%s)", i+1, f.Severity, f.Title, f.Layer, when)
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
	case len(v.Unexplained) > 0 && v.Health >= model.Degraded && !v.Explained:
		w.blank()
		w.line(0, "Not explained by a known pattern; look first at")
	case len(v.Unexplained) > 0:
		w.blank()
		w.line(0, "Also seen, not explained by the above")
	case len(v.Findings) == 0:
		w.line(1, "and nothing else looks wrong")
	}
	for _, s := range v.Unexplained {
		when := ""
		if !s.At.IsZero() {
			when = " (" + age(s.At, ev.Now) + ")"
		}
		w.line(1, "· %s: %s%s", s.Kind, s.Text, when)
	}
	for _, layer := range triage.Layers {
		w.blank()
		w.line(0, "%s", layerTitles[layer])
		switch layer {
		case triage.Operator:
			operatorEvidence(w, ev)
		case triage.Kubernetes:
			kubernetesEvidence(w, ev)
		case triage.Runtime:
			runtimeEvidence(w, ev)
		case triage.Performance:
			performanceEvidence(w, ev)
		case triage.HA:
			haEvidence(w, ev)
		}
		for _, g := range ev.Gaps {
			if g.Layer == layer {
				w.line(1, "not collected: %s: %s", g.What, g.Why)
			}
		}
	}
	return w.err
}

var layerTitles = map[triage.Layer]string{
	triage.Operator: "Operator", triage.Kubernetes: "Kubernetes", triage.Runtime: "Runtime",
	triage.Performance: "Performance", triage.HA: "HA",
}

func operatorEvidence(w *lineWriter, ev *triage.Evidence) {
	j := ev.Job
	w.line(1, "lifecycle %s, JobManager %s, job %s, reconciled %s, generation %d (observed %d)",
		dash(j.Lifecycle), dash(j.JMStatus), dash(j.JobState), age(j.ReconciledAt, ev.Now), j.Generation, j.ObservedGeneration)
	if e := j.Error; e != nil {
		w.line(1, "error: %s", e.Summary(w.cols()))
		for _, c := range e.Causes {
			w.line(2, "caused by %s: %s", shortType(c.Type), flink.FirstLine(c.Message))
		}
	}
	ownerEvidence(w, j, ev.Now)
}

// revertNotes say what the owner does with a manual edit of the spec.
var revertNotes = map[model.Reverts]string{
	model.RevertsAlways:      "; it reverts a manual edit of the spec",
	model.RevertsSelfHeal:    "; it reverts a manual edit of the spec when the Application has self-heal on",
	model.RevertsNextRelease: "; a manual edit of the spec lasts until the next release",
	model.RevertsUnknown:     "; whether it reverts a manual edit is not known",
}

// ownerEvidence names who the spec comes from and the fields changed outside it.
func ownerEvidence(w *lineWriter, j *model.Job, now time.Time) {
	if len(j.SpecOwners) > 0 {
		w.line(1, "spec written by %s, last %s", strings.Join(j.SpecOwners, ", "), age(j.SpecChangedAt, now))
	}
	o := j.Owner
	if o.Tool == "" {
		return
	}
	w.line(1, "owned by %s (from %s)%s", o, o.From, revertNotes[o.Reverts])
	for _, e := range o.Edits {
		w.line(2, "spec %s changed outside %s by %s, last %s", strings.Join(e.Fields, ", "), o.Tool, e.Manager, age(e.Time, now))
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
		lines := collapse(ev.Logs)
		for i, l := range lines {
			if i == whyLogs {
				rest := 0
				for _, r := range lines[i:] {
					rest += r.n
				}
				w.line(2, "… %d more: kubectl flink logs %s%s", rest, ev.Job.Key(), previousFlag(l.LogLine))
				break
			}
			if l.n > 1 {
				w.line(2, "%s (×%d)", l.Text, l.n)
			} else {
				w.line(2, "%s", l.Text)
			}
		}
	}
}

// repeated is a log line with the number of times it occurs.
type repeated struct {
	triage.LogLine
	n int
}

// collapse keeps each distinct line once, where it first occurs, with how often it occurs: a failure that repeats
// with every restart is one line, not a screenful.
func collapse(lines []triage.LogLine) []repeated {
	var out []repeated
	at := map[triage.LogLine]int{}
	for _, l := range lines {
		if i, ok := at[l]; ok {
			out[i].n++
			continue
		}
		at[l] = len(out)
		out = append(out, repeated{l, 1})
	}
	return out
}

func previousFlag(l triage.LogLine) string {
	if l.Previous {
		return " --previous"
	}
	return ""
}

func runtimeEvidence(w *lineWriter, ev *triage.Evidence) {
	snapshotEvidence(w, ev)
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

// snapshotEvidence summarises the job's FlinkStateSnapshots in one line; snapshots lists them.
func snapshotEvidence(w *lineWriter, ev *triage.Evidence) {
	if len(ev.Snapshots) == 0 {
		return
	}
	var completed *kube.FlinkStateSnapshot
	failing, pending := 0, 0
	for i := range ev.Snapshots {
		s := &ev.Snapshots[i]
		switch {
		case s.Status.State == "COMPLETED":
			if completed == nil || s.Result().After(completed.Result()) {
				completed = s
			}
		case s.Status.State == "":
			pending++
		case s.Status.State == "FAILED" || s.Status.State == "ABANDONED" || s.Status.Failures > 0:
			failing++
		}
	}
	line := fmt.Sprintf("%d FlinkStateSnapshots", len(ev.Snapshots))
	if completed != nil {
		line += fmt.Sprintf(", newest completed %s %s %s", strings.ToLower(completed.Type()), completed.Name, age(completed.Result(), ev.Now))
	}
	if failing > 0 {
		line += fmt.Sprintf(", %d failing", failing)
	}
	if pending > 0 {
		line += fmt.Sprintf(", %d without a status", pending)
	}
	w.line(1, "%s", line)
}

func performanceEvidence(w *lineWriter, ev *triage.Evidence) {
	if len(ev.Metrics) > 0 && ev.REST != nil && ev.REST.Job != nil && !triage.Settled(ev.REST.Job) {
		w.line(1, "rates average the last minute; not judged in the first %s after the job started", triage.Short(triage.Warmup))
	}
	for i := range ev.Metrics {
		v := &ev.Metrics[i]
		role := ""
		switch {
		case v.Source && v.Sink:
			role = " (source and sink)"
		case v.Source:
			role = " (source)"
		case v.Sink:
			role = " (sink)"
		}
		m := v.Metrics
		busy, pressed, idle := triage.WorstSplit(m)
		line := fmt.Sprintf("%s%s: in %.0f/s, out %.0f/s; busy %.0f, backpressured %.0f, idle %.0f ms/s", v.Name, role,
			m[triage.MetricIn].Sum, m[triage.MetricOut].Sum, busy, pressed, idle)
		if p, ok := v.Pending(); ok {
			line += fmt.Sprintf("; %.0f records pending", p)
		}
		w.line(1, "%s", line)
	}
}

func haEvidence(w *lineWriter, ev *triage.Evidence) {
	switch {
	case !ev.HAConfigured:
		w.line(1, "HA not configured")
	case ev.HA != nil:
		w.line(1, "ConfigMaps: %s", dash(strings.Join(ev.HA.ConfigMaps, ", ")))
		w.line(1, "job plans: %s; checkpoint pointers: %s", dash(strings.Join(ev.HA.Plans, ", ")), dash(strings.Join(ev.HA.Checkpoints, ", ")))
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

// lineWriter writes indented lines cut to width, whyWidth when 0, and keeps the first error.
type lineWriter struct {
	w     io.Writer
	width int
	err   error
}

func (l *lineWriter) cols() int { return cmp.Or(l.width, whyWidth) }

func (l *lineWriter) line(indent int, format string, args ...any) {
	if l.err != nil {
		return
	}
	s := strings.ReplaceAll(fmt.Sprintf(format, args...), "\n", " ")
	lead := len(s) - len(strings.TrimLeft(s, " "))
	s = truncate(strings.Repeat("  ", indent)+s[:lead]+strings.Join(strings.Fields(s), " "), l.cols())
	_, l.err = fmt.Fprintln(l.w, s)
}

// wrap writes prose over as many lines as it needs, cutting at spaces.
func (l *lineWriter) wrap(indent int, text string) {
	prefix := strings.Repeat("  ", indent)
	line := prefix
	for _, word := range strings.Fields(text) {
		if len(line) > len(prefix) && len(line)+1+len(word) > l.cols() {
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
