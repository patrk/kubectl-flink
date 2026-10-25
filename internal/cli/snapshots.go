package cli

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/liggitt/tabwriter"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

type snapshotsOptions struct {
	*Options
	restFlags
}

func newSnapshotsCommand(o *Options) *cobra.Command {
	so := &snapshotsOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "snapshots JOB",
		Short: "Show a job's savepoints and checkpoints: FlinkStateSnapshots and the JobManager's own record",
		Long: `List the job's FlinkStateSnapshot resources, newest first, then what the JobManager reports: the last
completed checkpoint, the last savepoint and what the job restored from. The JobManager's record also holds
savepoints taken through its REST API, which never appear as resources. JOB is namespace/name, a name, an ID
label value or a unique part of a name.`,
		Example: `  kubectl flink snapshots team-a/orders
  kubectl flink snapshots orders -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := so.validate(); err != nil {
				return err
			}
			return so.run(cmd.Context(), args[0])
		},
	}
	so.register(cmd, "Snapshots to show")
	return cmd
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
	Job        render.JobRef      `json:"job"`
	Snapshots  []SnapshotItem     `json:"snapshots"`
	JobManager *flink.Checkpoints `json:"jobManager,omitempty"`
	// NotCollected says why the JobManager's record or the resources could not be read.
	NotCollected []string `json:"notCollected,omitempty"`
}

func (so *snapshotsOptions) run(ctx context.Context, arg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := so.resolveJob(ctx, arg, so.allNamespaces)
	if err != nil {
		return err
	}
	rep := SnapshotReport{Job: render.NewJobRef(t.job), Snapshots: []SnapshotItem{}}
	snaps, err := triage.JobSnapshots(ctx, t.client, t.job)
	switch {
	case apierrors.IsNotFound(err):
		rep.NotCollected = append(rep.NotCollected, "FlinkStateSnapshots: the CRD is not installed")
	case err != nil:
		rep.NotCollected = append(rep.NotCollected, "FlinkStateSnapshots: "+err.Error())
	}
	for i := range snaps {
		if i == so.limit {
			break
		}
		rep.Snapshots = append(rep.Snapshots, snapshotItem(&snaps[i]))
	}
	rep.JobManager, err = so.jobManagerRecord(ctx, t)
	if err != nil {
		rep.NotCollected = append(rep.NotCollected, "JobManager: "+err.Error())
	}
	switch so.output {
	case "json":
		return render.JSON(so.Out, rep)
	case "yaml":
		return render.YAML(so.Out, rep)
	}
	return printSnapshots(so.Out, &rep, so.now())
}

// jobManagerRecord reads the JobManager's checkpoint record, when the JobManager is up or a REST URL is given.
func (so *snapshotsOptions) jobManagerRecord(ctx context.Context, t *target) (*flink.Checkpoints, error) {
	var cps *flink.Checkpoints
	err := t.withJobManager(ctx, so.restURL, func(c *flink.Client) error {
		var err error
		cps, err = c.Checkpoints(ctx, t.job.JobID)
		return err
	})
	return cps, err
}

func snapshotItem(s *kube.FlinkStateSnapshot) SnapshotItem {
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

func printSnapshots(out io.Writer, rep *SnapshotReport, now time.Time) error {
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
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", s.Name, s.Type, cmp.Or(s.Trigger, "-"), state, ago(when, now), detail)
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
			w.printf("  %s: #%d %s, %s\n", label, c.ID, ago(c.LatestAck.Time(), now), location(c.ExternalPath))
		}
		cp("last completed checkpoint", cps.Latest.Completed)
		cp("last savepoint", cps.Latest.Savepoint)
		if r := cps.Latest.Restored; r != nil {
			w.printf("  restored from: #%d %s, %s\n", r.ID, ago(r.RestoredAt.Time(), now), location(r.ExternalPath))
		} else {
			w.printf("  restored from: -\n")
		}
	}
	for _, g := range rep.NotCollected {
		w.printf("  not collected: %s\n", g)
	}
	return w.err
}
