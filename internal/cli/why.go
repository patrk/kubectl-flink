package cli

import (
	"context"
	"errors"
	"slices"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

type whyOptions struct {
	*Options
	restFlags
	exitCode bool
}

func newWhyCommand(o *Options) *cobra.Command {
	wo := &whyOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "why JOB",
		Short: "Explain why a job is unhealthy",
		Long: `Collect the operator status, pods, events, JobManager logs and REST API of a job, name the known
failure patterns among them, and show the evidence by layer. When no pattern matches, the strongest signals are
listed first. JOB is namespace/name, a name, an ID label value or a unique part of a name.`,
		Example: `  kubectl flink why team-a/orders
  kubectl flink why orders -o json
  kubectl flink why orders --exit-code && echo healthy`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := wo.validate(); err != nil {
				return err
			}
			return wo.run(cmd.Context(), args[0])
		},
	}
	wo.register(cmd, "")
	_ = cmd.Flags().MarkHidden("limit")
	cmd.Flags().BoolVar(&wo.exitCode, "exit-code", false, "Exit with 4 when the job is unhealthy or a finding is DEGRADED or worse")
	return cmd
}

func (wo *whyOptions) run(ctx context.Context, arg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := wo.resolveJob(ctx, arg, wo.allNamespaces)
	if err != nil {
		return err
	}
	prof := t.profile
	rules, err := prof.VerdictRules(triage.DefaultRules())
	if err != nil {
		return err
	}
	access, closeFn := t.access(wo.restURL)
	defer closeFn()
	src := triage.Source{Kube: t.client, REST: access, ForceREST: wo.restURL != ""}
	ev := triage.Collect(ctx, src, t.job, wo.now(), prof.Thresholds())
	signals := triage.Signals(ev)
	v, err := triage.Judge(ev, signals, rules)
	if err != nil {
		return err
	}
	switch wo.output {
	case "json":
		err = render.JSON(wo.Out, render.NewWhyReport(ev, v, signals))
	case "yaml":
		err = render.YAML(wo.Out, render.NewWhyReport(ev, v, signals))
	default:
		err = render.Why(wo.Out, ev, v)
	}
	if err != nil {
		return err
	}
	if wo.exitCode && (v.Health >= model.Degraded || slices.ContainsFunc(v.Findings, func(f triage.Finding) bool { return f.Severity >= model.Degraded })) {
		return &exitError{code: ExitUnhealthy, err: errors.New("the job is unhealthy")}
	}
	return nil
}
