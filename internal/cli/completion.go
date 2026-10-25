package cli

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
)

// completionTimeout keeps tab completion responsive on a slow API server; nothing is offered when it runs out.
const completionTimeout = 3 * time.Second

// completeJobs offers the jobs in the current scopes whose namespace/name or bare name starts with what was typed:
// shells keep only candidates that extend the word, so a substring match would be thrown away.
func (o *Options) completeJobs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	all, _ := cmd.Flags().GetBool("all-namespaces")
	srcs, _, err := o.sources(all)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()
	var out []string
	scopes := fleet.Load(ctx, srcs, kube.Query{})
	for i := range scopes {
		for j := range scopes[i].Jobs {
			job := &scopes[i].Jobs[j]
			switch {
			case strings.HasPrefix(job.Key(), toComplete):
				out = append(out, job.Key())
			case strings.HasPrefix(job.Name, toComplete):
				out = append(out, job.Name)
			}
		}
	}
	// The same job in several contexts is one candidate.
	slices.Sort(out)
	return slices.Compact(out), cobra.ShellCompDirectiveNoFileComp
}

// completeContexts offers the kubeconfig's contexts.
func (o *Options) completeContexts(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	cs, err := kube.NewContexts(o.ConfigFlags)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return cs.Names(), cobra.ShellCompDirectiveNoFileComp
}

// registerCompletions wires completion for job arguments and the --context flag.
func (o *Options) registerCompletions(root *cobra.Command) {
	_ = root.RegisterFlagCompletionFunc("context", o.completeContexts)
	for _, c := range root.Commands() {
		if strings.HasSuffix(c.Use, " JOB") {
			c.ValidArgsFunction = o.completeJobs
		}
	}
}
