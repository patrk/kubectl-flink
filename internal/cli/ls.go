package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/profile"
	"github.com/patrk/kubectl-flink/internal/render"
)

// reassessEvery re-derives health in -w without events, because a job turns DEGRADED or STUCK by time alone.
const reassessEvery = 5 * time.Second

type lsOptions struct {
	*Options
	fleetFlags
	output    string
	watch     bool
	noHeaders bool
	exitCode  bool

	filter fleet.Filter
}

func newLsCommand(o *Options) *cobra.Command {
	lo := &lsOptions{Options: o}
	cmd := &cobra.Command{
		Use:     "ls [FILTER]",
		Aliases: []string{"list"},
		Short:   "List Flink jobs with their health",
		Long: `List FlinkDeployments and FlinkSessionJobs with a health derived from the operator status, the
JobManager and TaskManager pods, and how long each state has lasted. FILTER keeps jobs whose namespace/name
contains it. The summary and any fallback notes go to stderr.`,
		Example: `  kubectl flink ls -A --unhealthy
  kubectl flink ls -n team-a -o wide --sort state-age
  kubectl flink ls -l team=payments -o json
  kubectl flink ls -A -w`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				lo.filter.Text = args[0]
			}
			if err := lo.validate(); err != nil {
				return &exitError{code: ExitUsage, err: err}
			}
			return lo.run(cmd.Context())
		},
	}
	f := cmd.Flags()
	f.StringVarP(&lo.output, "output", "o", "table", "Output format: "+strings.Join(render.Formats, ", "))
	f.BoolVarP(&lo.watch, "watch", "w", false, "Keep watching and print jobs whose state changes")
	f.BoolVar(&lo.noHeaders, "no-headers", false, "Print no header, summary or notes")
	f.BoolVar(&lo.exitCode, "exit-code", false, "Exit with 4 when a listed job is DEGRADED, STUCK or FAILED")
	_ = cmd.RegisterFlagCompletionFunc("output", cobra.FixedCompletions(render.Formats, cobra.ShellCompDirectiveNoFileComp))
	lo.register(cmd)
	return cmd
}

func (lo *lsOptions) validate() error {
	if !slices.Contains(render.Formats, lo.output) {
		return fmt.Errorf("unknown output %q, use one of %s", lo.output, strings.Join(render.Formats, ", "))
	}
	if lo.watch && lo.output == "name" {
		return errors.New("-o name cannot be watched")
	}
	err := lo.parse()
	lo.filter.Health = lo.healthValues
	return err
}

// sources reads the kubeconfig and the profile and builds a client per scope.
func (o *Options) sources(allNamespaces bool) ([]fleet.Source, *profile.Profile, error) {
	cs, err := kube.NewContexts(o.ConfigFlags)
	if err != nil {
		return nil, nil, err
	}
	path, err := profile.Path()
	if err != nil {
		return nil, nil, err
	}
	file, err := profile.Load(path)
	if err != nil {
		return nil, nil, err
	}
	p := file.For(cs.Current())
	fl := profile.Flags{AllNamespaces: allNamespaces}
	if c := o.ConfigFlags.Context; c != nil {
		fl.Context = *c
	}
	if n := o.ConfigFlags.Namespace; n != nil {
		fl.Namespace = *n
	}
	srcs := fleet.Sources(cs, p.Resolve(cs.Current(), fl))
	for i := range srcs {
		srcs[i].Owners = p.Owners
	}
	return srcs, &p, nil
}

func (lo *lsOptions) run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	srcs, prof, err := lo.sources(lo.allNamespaces)
	if err != nil {
		return err
	}
	th := prof.Thresholds()
	lo.filter.IDLabels = prof.IDLabels
	if lo.watch {
		return lo.runWatch(ctx, srcs, th)
	}
	scopes := fleet.Load(ctx, srcs, lo.query())
	if len(scopes) == 1 && scopes[0].Err != nil {
		return scopes[0].Err
	}
	rows, err := lo.rows(scopes, th)
	if err != nil {
		return err
	}
	if err := lo.print(rows, scopes); err != nil {
		return err
	}
	for i := range scopes {
		if scopes[i].Err != nil {
			return &exitError{code: ExitError, err: fmt.Errorf("context %s could not be read", scopes[i].Context)}
		}
	}
	if lo.exitCode && slices.ContainsFunc(rows, func(r fleet.Row) bool { return fleet.Unhealthy(r.Health) }) {
		return &exitError{code: ExitUnhealthy, err: errors.New("unhealthy jobs listed")}
	}
	return nil
}

func (lo *lsOptions) rows(scopes []fleet.Scope, th model.Thresholds) ([]fleet.Row, error) {
	rows := lo.filter.Apply(fleet.Rows(scopes, lo.now(), th))
	return rows, fleet.Sort(rows, lo.sortBy)
}

func (lo *lsOptions) tableOptions(scopes []fleet.Scope) render.TableOptions {
	o := render.ScopeOptions(scopes)
	o.Wide, o.NoHeaders, o.Color = lo.output == "wide", lo.noHeaders, lo.color()
	return o
}

func (lo *lsOptions) print(rows []fleet.Row, scopes []fleet.Scope) error {
	switch lo.output {
	case "json":
		return render.JSON(lo.Out, render.NewList(rows, scopes))
	case "yaml":
		return render.YAML(lo.Out, render.NewList(rows, scopes))
	case "name":
		return render.Names(lo.Out, rows)
	}
	if err := render.NewTable(lo.Out, lo.tableOptions(scopes)).Print(rows, scopes, lo.now()); err != nil {
		return err
	}
	if !lo.noHeaders {
		lo.notes(scopes)
		s := render.Summarise(rows)
		_, _ = fmt.Fprintln(lo.ErrOut, s.Line())
	}
	return nil
}

func (lo *lsOptions) notes(scopes []fleet.Scope) {
	for _, n := range render.Notes(scopes) {
		_, _ = fmt.Fprintln(lo.ErrOut, "note: "+n)
	}
}

func (lo *lsOptions) runWatch(ctx context.Context, srcs []fleet.Source, th model.Thresholds) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	updates := make(chan []fleet.Scope)
	go fleet.Watch(ctx, srcs, lo.query(), updates)

	var scopes []fleet.Scope
	select {
	case scopes = <-updates:
	case <-ctx.Done():
		return nil
	}
	// Only a scope that could not be read at all ends the watch; a failing or slow watch is reported and kept.
	if len(scopes) == 1 && scopes[0].Err != nil {
		return scopes[0].Err
	}
	var update func([]fleet.Row, []fleet.Scope) error
	if lo.output == "json" || lo.output == "yaml" {
		ev := render.NewEvents(lo.Out, lo.output == "yaml")
		update = ev.Update
	} else {
		t := render.NewTable(lo.Out, lo.tableOptions(scopes))
		first := true
		update = func(rows []fleet.Row, scopes []fleet.Scope) error {
			if first {
				first = false
				if !lo.noHeaders {
					lo.notes(scopes)
				}
				return t.Print(rows, scopes, lo.now())
			}
			return t.Update(rows, scopes, lo.now())
		}
	}
	tick := time.NewTicker(reassessEvery)
	defer tick.Stop()
	watchErrs := map[string]string{}
	for {
		lo.reportWatchErrors(scopes, watchErrs)
		rows, err := lo.rows(scopes, th)
		if err != nil {
			return err
		}
		if err := update(rows, scopes); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case scopes = <-updates:
		case <-tick.C:
		}
	}
}

// reportWatchErrors writes a line to stderr when a scope's watch starts failing and when it recovers, so that a
// table showing stale rows says so whatever the number of scopes.
func (lo *lsOptions) reportWatchErrors(scopes []fleet.Scope, last map[string]string) {
	for i := range scopes {
		s := &scopes[i]
		msg := ""
		if s.WatchErr != nil {
			msg = s.WatchErr.Error()
		}
		switch prev := last[s.Context]; {
		case msg != "" && msg != prev:
			_, _ = fmt.Fprintf(lo.ErrOut, "warning: context %s: %s; rows may be stale\n", s.Context, msg)
		case msg == "" && prev != "":
			_, _ = fmt.Fprintf(lo.ErrOut, "context %s: watching again\n", s.Context)
		}
		last[s.Context] = msg
	}
}

// query reads snapshots only for the outputs that show them: the wide table, JSON and YAML.
func (lo *lsOptions) query() kube.Query {
	return kube.Query{Selector: lo.selector, Snapshots: lo.output == "wide" || lo.output == "json" || lo.output == "yaml"}
}

// color is true when stdout is a terminal and NO_COLOR is unset.
func (o *Options) color() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	f, ok := o.Out.(*os.File)
	return ok && term.IsTerminal(int(f.Fd()))
}
