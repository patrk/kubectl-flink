package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

type graphOptions struct {
	*Options
	restFlags
}

func newGraphCommand(o *Options) *cobra.Command {
	g := &graphOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "graph JOB",
		Short: "Show a job's operators with their busy, back-pressured and idle time",
		Long: `Show the job's operators from the sources down, each with its parallelism, the time split of its worst
subtask and its rates, coloured as the Flink web UI colours its job graph: busy red, back-pressured dark grey,
idle blue. The bottleneck is the busy operator behind back pressure, the one why names.`,
		Example: `  kubectl flink graph team-a/orders
  kubectl flink graph orders -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := g.validate(); err != nil {
				return err
			}
			return g.run(cmd.Context(), args[0])
		},
	}
	g.register(cmd, "")
	_ = cmd.Flags().MarkHidden("limit")
	return cmd
}

func (g *graphOptions) run(ctx context.Context, arg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := g.resolveJob(ctx, arg, g.allNamespaces)
	if err != nil {
		return err
	}
	access, closeFn := t.access(g.restURL)
	defer closeFn()
	// Only the job and its vertex metrics: the rest of why's collection is not drawn.
	ev := triage.CollectGraph(ctx, triage.Source{Kube: t.client, REST: access, ForceREST: g.restURL != ""}, t.job, g.now())
	switch g.output {
	case "json":
		return render.JSON(g.Out, render.NewJobGraph(ev))
	case "yaml":
		return render.YAML(g.Out, render.NewJobGraph(ev))
	}
	return render.Graph(g.Out, ev, g.color())
}
