package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/render"
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
	rep := render.NewCheckpointReport(t.job, cps, cfg, co.now(), co.limit)
	switch co.output {
	case "json":
		return render.JSON(co.Out, rep)
	case "yaml":
		return render.YAML(co.Out, rep)
	}
	return render.Checkpoints(co.Out, &rep, co.now())
}
