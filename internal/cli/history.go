package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/history"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

type historyOptions struct {
	*Options
	restFlags
}

func newHistoryCommand(o *Options) *cobra.Command {
	ho := &historyOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "history JOB",
		Short: "Show what happened to a job, and spec changes not applied yet",
		Long: `Reconstruct a job's history from what the cluster still remembers: events (kept about an hour), who
wrote the spec and when, status timestamps, container exits, and the JobManager's failures and restores. Then list
the differences between the spec and what the operator last deployed. JOB is namespace/name, a name, an ID label
value or a unique part of a name.`,
		Example: `  kubectl flink history team-a/orders
  kubectl flink history orders -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := ho.validate(); err != nil {
				return err
			}
			return ho.run(cmd.Context(), args[0])
		},
	}
	ho.register(cmd, "")
	_ = cmd.Flags().MarkHidden("limit")
	return cmd
}

// collectJob gathers why's evidence and the raw resource, which history, report and top's detail all need. force
// asks the JobManager whatever the resource says of it, as --rest-url does.
func (o *Options) collectJob(ctx context.Context, t *target, access flink.Access, force bool) (ev *triage.Evidence, rawCR []byte) {
	src := triage.Source{Kube: t.client, REST: access, ForceREST: force}
	ev = triage.Collect(ctx, src, t.job, o.now(), t.profile.Thresholds())
	resource := kube.Deployments
	if t.job.Kind == model.KindSessionJob {
		resource = kube.SessionJobs
	}
	rawCR, err := t.client.GetRaw(ctx, resource, t.job.Namespace, t.job.Name)
	if err != nil {
		ev.Gaps = append(ev.Gaps, triage.Gap{Layer: triage.Operator, What: "resource", Why: err.Error()})
	}
	return ev, rawCR
}

func (ho *historyOptions) run(ctx context.Context, arg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := ho.resolveJob(ctx, arg, ho.allNamespaces)
	if err != nil {
		return err
	}
	r, err := t.profile.Redactor()
	if err != nil {
		return err
	}
	access, closeFn := t.access(ho.restURL)
	defer closeFn()
	ev, raw := ho.collectJob(ctx, t, access, ho.restURL != "")
	h, err := history.Build(ev, raw, r)
	if err != nil {
		return err
	}
	ref := render.NewJobRef(ev.Job)
	switch ho.output {
	case "json":
		return render.JSON(ho.Out, render.HistoryReport{Job: ref, History: h})
	case "yaml":
		return render.YAML(ho.Out, render.HistoryReport{Job: ref, History: h})
	}
	return render.History(ho.Out, ref, h, ho.now())
}
