package cli

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/liggitt/tabwriter"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

type checkpointsOptions struct {
	*Options
	restFlags
}

func newCheckpointsCommand(o *Options) *cobra.Command {
	co := &checkpointsOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "checkpoints JOB",
		Short: "Show a job's checkpoint configuration, latest checkpoints and history",
		Long: `Show how often the job checkpoints, when the last checkpoint completed and failed, what it restored
from, and the recent history. A job whose last completed checkpoint is many intervals old commits nothing to
transactional sinks. JOB is namespace/name, a name, an ID label value or a unique part of a name.`,
		Example: `  kubectl flink checkpoints team-a/orders
  kubectl flink checkpoints orders --limit 30 -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := co.validate(); err != nil {
				return err
			}
			return co.withJobManager(cmd.Context(), args[0], &co.restFlags, co.run)
		},
	}
	co.register(cmd, "History entries to show")
	return cmd
}

// CheckpointReport is the -o json and -o yaml document of checkpoints.
type CheckpointReport struct {
	Job     render.JobRef           `json:"job"`
	Config  *flink.CheckpointConfig `json:"config,omitempty"`
	Summary *flink.Checkpoints      `json:"checkpoints"`
	Stale   string                  `json:"stale,omitempty"`
}

func (co *checkpointsOptions) run(ctx context.Context, t *target, c *flink.Client) error {
	cps, err := c.Checkpoints(ctx, t.job.JobID)
	if err != nil {
		return err
	}
	cfg, err := c.CheckpointConfig(ctx, t.job.JobID)
	if flink.IsNotFound(err) {
		cfg, err = nil, nil // checkpointing is off
	}
	if err != nil {
		return err
	}
	if len(cps.History) > co.limit {
		cps.History = cps.History[:co.limit]
	}
	now := co.now()
	rep := CheckpointReport{Job: render.NewJobRef(t.job), Config: cfg, Summary: cps}
	// Only a running job is expected to keep checkpointing; a finished or failed one has an old checkpoint.
	if t.job.JobState == "RUNNING" {
		if s := triage.StaleCheckpoint(cps, cfg, now); s != "" {
			rep.Stale = s + ": transactional sinks commit nothing new"
		}
	}
	switch co.output {
	case "json":
		return render.JSON(co.Out, rep)
	case "yaml":
		return render.YAML(co.Out, rep)
	}
	return printCheckpoints(co.Out, &rep, now)
}

func short(d time.Duration) string { return triage.Short(d) }

func ago(t, now time.Time) string {
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

func printCheckpoints(out io.Writer, rep *CheckpointReport, now time.Time) error {
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
			p("%s\t#%d %s: %s", label, c.ID, ago(c.FailedAt.Time(), now), flink.FirstLine(c.FailureMessage))
			return
		}
		p("%s\t#%d %s, took %s, %s", label, c.ID, ago(at, now), short(time.Duration(c.Duration)*time.Millisecond), byteSize(c.CheckpointedSz))
	}
	cp("Last completed", s.Latest.Completed)
	cp("Last failed", s.Latest.Failed)
	cp("Last savepoint", s.Latest.Savepoint)
	if r := s.Latest.Restored; r != nil {
		kind := "checkpoint"
		if r.IsSavepoint {
			kind = "savepoint"
		}
		p("Restored from\t%s #%d %s, %s", kind, r.ID, ago(r.RestoredAt.Time(), now), location(r.ExternalPath))
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
		p("%d\t%s\t%s\t%s\t%s\t%s\t%d/%d", c.ID, c.Status, typ, ago(c.Triggered.Time(), now),
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
