package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/render"
)

type exceptionsOptions struct {
	*Options
	restFlags
	full bool
}

func newExceptionsCommand(o *Options) *cobra.Command {
	eo := &exceptionsOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "exceptions JOB",
		Short: "Show a job's failure history from the JobManager",
		Long: `Show the job's exception history, newest first: when, which exception, in which task and on which
TaskManager, and the deepest cause. --full adds the stack traces. JOB is namespace/name, a name, an ID label
value or a unique part of a name.`,
		Example: `  kubectl flink exceptions team-a/orders
  kubectl flink exceptions orders --full --limit 3`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := eo.validate(); err != nil {
				return err
			}
			return eo.withJobManager(cmd.Context(), args[0], &eo.restFlags, eo.run)
		},
	}
	eo.register(cmd, "Failures to show")
	cmd.Flags().BoolVar(&eo.full, "full", false, "Print the stack traces")
	return cmd
}

func (eo *exceptionsOptions) run(ctx context.Context, t *target, c *flink.Client) error {
	ex, err := c.Exceptions(ctx, t.job.JobID, eo.limit)
	if err != nil {
		return err
	}
	rep := render.NewExceptionReport(t.job, ex.History.Entries, ex.History.Truncated)
	switch eo.output {
	case "json":
		return render.JSON(eo.Out, rep)
	case "yaml":
		return render.YAML(eo.Out, rep)
	}
	return render.Exceptions(eo.Out, &rep, eo.full, eo.now())
}
