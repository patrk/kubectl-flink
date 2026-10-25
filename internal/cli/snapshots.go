package cli

import (
	"context"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/patrk/kubectl-flink/internal/flink"
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

func (so *snapshotsOptions) run(ctx context.Context, arg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := so.resolveJob(ctx, arg, so.allNamespaces)
	if err != nil {
		return err
	}
	rep := render.SnapshotReport{Job: render.NewJobRef(t.job), Snapshots: []render.SnapshotItem{}}
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
		rep.Snapshots = append(rep.Snapshots, render.NewSnapshotItem(&snaps[i]))
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
	return render.Snapshots(so.Out, &rep, so.now())
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
