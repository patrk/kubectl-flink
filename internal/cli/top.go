package cli

import (
	"context"
	"errors"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/tui"
)

type topOptions struct {
	*Options
	fleetFlags
}

func newTopCommand(o *Options) *cobra.Command {
	to := &topOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "top [FILTER]",
		Short: "Watch Flink jobs and their health in a terminal UI",
		Long: `Show the jobs ls lists as a live table: worst health first, kept current by watches. FILTER starts a
search by namespace/name or ID label value. Keys follow k9s; press ? for the list.`,
		Example: `  kubectl flink top -A
  kubectl flink top -A --unhealthy
  kubectl flink top -l team=payments 4711`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			search := ""
			if len(args) == 1 {
				search = args[0]
			}
			if err := to.check(); err != nil {
				return &exitError{code: ExitUsage, err: err}
			}
			return to.run(cmd.Context(), search)
		},
	}
	to.register(cmd)
	return cmd
}

func (to *topOptions) check() error {
	if err := to.parse(); err != nil {
		return err
	}
	in, inOK := to.In.(*os.File)
	out, outOK := to.Out.(*os.File)
	if !inOK || !outOK || !term.IsTerminal(int(in.Fd())) || !term.IsTerminal(int(out.Fd())) {
		return errors.New("top needs a terminal; use kubectl flink ls -w to follow jobs in a pipe")
	}
	return nil
}

func (to *topOptions) run(ctx context.Context, search string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	srcs, prof, err := to.sources(to.allNamespaces)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	updates := make(chan []fleet.Scope)
	go func() {
		// Snapshots are left out: top does not show their age.
		fleet.Watch(ctx, srcs, kube.Query{Selector: to.selector}, updates)
		close(updates)
	}()
	return tui.Run(ctx, tui.Config{
		Updates: updates, Now: to.now, Thresholds: prof.Thresholds(), IDLabels: prof.IDLabels,
		Search: search, Health: to.healthValues, Sort: to.sortBy,
	}, to.In, to.Out)
}
