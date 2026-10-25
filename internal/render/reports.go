package render

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/liggitt/tabwriter"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// The reports of the single-job commands that read the JobManager: their -o json documents and their text,
// shared by the commands and top's detail screen.

// ExceptionReport is the -o json and -o yaml document of exceptions.
type ExceptionReport struct {
	Job       JobRef            `json:"job"`
	Entries   []flink.Exception `json:"entries"`
	Truncated bool              `json:"truncated,omitempty"`
}

// shortClass drops the package from "org.example.FooException: message".
func shortClass(s string) string {
	cls, msg, found := strings.Cut(s, ": ")
	if i := strings.LastIndex(cls, "."); i >= 0 && !strings.Contains(cls, " ") {
		cls = cls[i+1:]
	}
	if found {
		return cls + ": " + msg
	}
	return cls
}

// Exceptions prints the exception report; full adds the stack traces.
func Exceptions(out io.Writer, rep *ExceptionReport, full bool, now time.Time) error {
	w := &errWriter{w: out}
	w.printf("Job: %s\n", rep.Job)
	if len(rep.Entries) == 0 {
		w.printf("No failures since the job started.\n")
		return w.err
	}
	more := ""
	if rep.Truncated {
		more = ", older ones truncated"
	}
	noun := "failures"
	if len(rep.Entries) == 1 {
		noun = "failure"
	}
	w.printf("%d %s, newest first%s\n", len(rep.Entries), noun, more)
	for i := range rep.Entries {
		e := &rep.Entries[i]
		w.printf("\n%s  %s\n", since(e.Timestamp.Time(), now), shortClass(e.Message()))
		if cause := e.Cause(); cause != "" {
			w.printf("  caused by: %s\n", shortClass(cause))
		}
		if e.TaskName != "" {
			w.printf("  task: %s\n", e.TaskName)
		}
		if where := e.Where(); where != "" {
			w.printf("  on: %s\n", where)
		}
		for _, k := range slices.Sorted(maps.Keys(e.FailureLabels)) {
			w.printf("  label %s: %s\n", k, e.FailureLabels[k])
		}
		if n := len(e.Concurrent); n > 0 {
			w.printf("  with %d concurrent failures\n", n)
		}
		if full {
			w.printf("\n%s\n", indent(strings.TrimSpace(e.Stacktrace), "    "))
		}
	}
	return w.err
}

func indent(s, prefix string) string { return prefix + strings.ReplaceAll(s, "\n", "\n"+prefix) }

// errWriter keeps the first write error, so printing code stays linear.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err == nil {
		_, e.err = fmt.Fprintf(e.w, format, args...)
	}
}

// CheckpointReport is the -o json and -o yaml document of checkpoints.
type CheckpointReport struct {
	Job     JobRef                  `json:"job"`
	Config  *flink.CheckpointConfig `json:"config,omitempty"`
	Summary *flink.Checkpoints      `json:"checkpoints"`
	Stale   string                  `json:"stale,omitempty"`
}

func short(d time.Duration) string { return triage.Short(d) }

func since(t, now time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return duration.HumanDuration(now.Sub(t)) + " ago"
}

func byteSize(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + "B"
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// Checkpoints prints the checkpoint report.
func Checkpoints(out io.Writer, rep *CheckpointReport, now time.Time) error {
	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format+"\n", args...) }
	p("%s\t%s", "Job", rep.Job.String())
	if cfg := rep.Config; cfg == nil || cfg.Interval == triage.PeriodicOff {
		p("Checkpointing\toff")
	} else {
		p("Checkpointing\tevery %s, timeout %s, %s, %s, %s", short(time.Duration(cfg.Interval)*time.Millisecond),
			short(time.Duration(cfg.Timeout)*time.Millisecond), cfg.Mode, cfg.StateBackend, cfg.CheckpointStorage)
	}
	s := rep.Summary
	cp := func(label string, c *flink.Checkpoint) {
		if c == nil {
			p("%s\t-", label)
			return
		}
		at := c.LatestAck.Time()
		if c.Status == "FAILED" {
			p("%s\t#%d %s: %s", label, c.ID, since(c.FailedAt.Time(), now), flink.FirstLine(c.FailureMessage))
			return
		}
		p("%s\t#%d %s, took %s, %s", label, c.ID, since(at, now), short(time.Duration(c.Duration)*time.Millisecond), byteSize(c.CheckpointedSz))
	}
	cp("Last completed", s.Latest.Completed)
	cp("Last failed", s.Latest.Failed)
	cp("Last savepoint", s.Latest.Savepoint)
	if r := s.Latest.Restored; r != nil {
		kind := "checkpoint"
		if r.IsSavepoint {
			kind = "savepoint"
		}
		p("Restored from\t%s #%d %s, %s", kind, r.ID, since(r.RestoredAt.Time(), now), location(r.ExternalPath))
	} else {
		p("Restored from\t-")
	}
	p("Counts\t%d completed, %d failed, %d in progress, %d restores", s.Counts.Completed, s.Counts.Failed, s.Counts.InProgress, s.Counts.Restored)
	if rep.Stale != "" {
		p("Warning\t%s", rep.Stale)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if len(s.History) == 0 {
		return nil
	}
	_, _ = fmt.Fprintln(out)
	w = tabwriter.NewWriter(out, 0, 8, 3, ' ', 0)
	p("ID\tSTATUS\tTYPE\tTRIGGERED\tDURATION\tSIZE\tACKED")
	for i := range s.History {
		c := &s.History[i]
		typ := c.Type
		if c.IsSavepoint {
			typ = "SAVEPOINT"
		}
		p("%d\t%s\t%s\t%s\t%s\t%s\t%d/%d", c.ID, c.Status, typ, since(c.Triggered.Time(), now),
			short(time.Duration(c.Duration)*time.Millisecond), byteSize(c.CheckpointedSz), c.AckedSubtasks, c.Subtasks)
	}
	return w.Flush()
}

// location shows where a checkpoint is stored; Flink reports a placeholder for one kept in JobManager memory.
func location(path string) string {
	if path == "" || path == "<checkpoint-not-externally-addressable>" {
		return "not externally addressable"
	}
	return path
}

// SnapshotItem is one FlinkStateSnapshot in the output.
type SnapshotItem struct {
	Name      string     `json:"name"`
	Type      string     `json:"type"`
	Trigger   string     `json:"trigger,omitempty"`
	State     string     `json:"state,omitempty"`
	Failures  int        `json:"failures,omitempty"`
	Path      string     `json:"path,omitempty"`
	Error     string     `json:"error,omitempty"`
	Created   time.Time  `json:"created"`
	Triggered *time.Time `json:"triggered,omitempty"`
	Result    *time.Time `json:"result,omitempty"`
}

// SnapshotReport is the -o json and -o yaml document of snapshots.
type SnapshotReport struct {
	Job        JobRef             `json:"job"`
	Snapshots  []SnapshotItem     `json:"snapshots"`
	JobManager *flink.Checkpoints `json:"jobManager,omitempty"`
	// NotCollected says why the JobManager's record or the resources could not be read.
	NotCollected []string `json:"notCollected,omitempty"`
}

// NewSnapshotItem is the output of one FlinkStateSnapshot.
func NewSnapshotItem(s *kube.FlinkStateSnapshot) SnapshotItem {
	it := SnapshotItem{Name: s.Name, Type: s.Type(), Trigger: s.Trigger(), State: s.Status.State, Failures: s.Status.Failures,
		Path: s.Status.Path, Created: s.CreationTimestamp.UTC()}
	if t := s.Triggered(); !t.IsZero() {
		it.Triggered = &t
	}
	if t := s.Result(); !t.IsZero() {
		it.Result = &t
	}
	if e := model.ParseStatusError(s.Status.Error); e != nil {
		it.Error = e.Summary(300)
	}
	return it
}

// Snapshots prints the snapshot report.
func Snapshots(out io.Writer, rep *SnapshotReport, now time.Time) error {
	w := &errWriter{w: out}
	w.printf("Job: %s\n\n", rep.Job)
	unprocessed := 0
	if len(rep.Snapshots) == 0 {
		w.printf("No FlinkStateSnapshots for this job.\n")
	} else {
		tw := tabwriter.NewWriter(out, 0, 8, 3, ' ', 0)
		_, _ = fmt.Fprintln(tw, "NAME\tTYPE\tTRIGGER\tSTATE\tAGE\tPATH OR ERROR")
		for i := range rep.Snapshots {
			s := &rep.Snapshots[i]
			state := cmp.Or(s.State, "-")
			if s.Failures > 0 && s.State != "COMPLETED" {
				state += " (" + strconv.Itoa(s.Failures) + " failures)"
			}
			if s.State == "" && now.Sub(s.Created) > triage.IgnoredAfter {
				unprocessed++
			}
			when := s.Created
			if s.Result != nil {
				when = *s.Result
			}
			detail := cmp.Or(s.Path, flink.FirstLine(s.Error), "-")
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", s.Name, s.Type, cmp.Or(s.Trigger, "-"), state, since(when, now), detail)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	if unprocessed > 0 {
		w.printf("\n%d without a status: the operator does not process them; see kubectl flink why %s/%s\n", unprocessed, rep.Job.Namespace, rep.Job.Name)
	}
	w.printf("\nJobManager\n")
	if cps := rep.JobManager; cps != nil {
		cp := func(label string, c *flink.Checkpoint) {
			if c == nil {
				w.printf("  %s: -\n", label)
				return
			}
			w.printf("  %s: #%d %s, %s\n", label, c.ID, since(c.LatestAck.Time(), now), location(c.ExternalPath))
		}
		cp("last completed checkpoint", cps.Latest.Completed)
		cp("last savepoint", cps.Latest.Savepoint)
		if r := cps.Latest.Restored; r != nil {
			w.printf("  restored from: #%d %s, %s\n", r.ID, since(r.RestoredAt.Time(), now), location(r.ExternalPath))
		} else {
			w.printf("  restored from: -\n")
		}
	}
	for _, g := range rep.NotCollected {
		w.printf("  not collected: %s\n", g)
	}
	return w.err
}

// NewCheckpointReport is the checkpoint report of job at now, with at most limit checkpoints of history. Only a
// running job is expected to keep checkpointing; a finished or failed one has an old checkpoint.
func NewCheckpointReport(job *model.Job, cps *flink.Checkpoints, cfg *flink.CheckpointConfig, now time.Time, limit int) CheckpointReport {
	if limit > 0 && len(cps.History) > limit {
		c := *cps
		c.History = c.History[:limit]
		cps = &c
	}
	rep := CheckpointReport{Job: NewJobRef(job), Config: cfg, Summary: cps}
	if job.JobState == "RUNNING" {
		if s := triage.StaleCheckpoint(cps, cfg, now); s != "" {
			rep.Stale = s + ": transactional sinks commit nothing new"
		}
	}
	return rep
}

// NewExceptionReport is the exception report of job.
func NewExceptionReport(job *model.Job, entries []flink.Exception, truncated bool) ExceptionReport {
	if entries == nil {
		entries = []flink.Exception{}
	}
	return ExceptionReport{Job: NewJobRef(job), Entries: entries, Truncated: truncated}
}
