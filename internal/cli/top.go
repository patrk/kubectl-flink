package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/history"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/profile"
	"github.com/patrk/kubectl-flink/internal/redact"
	"github.com/patrk/kubectl-flink/internal/report"
	"github.com/patrk/kubectl-flink/internal/triage"
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
		// Snapshots are left out: watching every completed one costs more than their age in w is worth.
		fleet.Watch(ctx, srcs, kube.Query{Selector: to.selector, Failures: true}, updates)
		close(updates)
	}()
	loader, err := newTopLoader(to.Options, srcs, prof)
	if err != nil {
		return err
	}
	defer loader.close()
	return tui.Run(ctx, tui.Config{
		Updates: updates, Now: to.now, Thresholds: prof.Thresholds(), IDLabels: prof.IDLabels,
		Search: search, Health: to.healthValues, Sort: to.sortBy, Loader: loader, Context: ctx,
	}, to.In, to.Out)
}

// topPoolSize is how many port-forwards top keeps open per context: the jobs looked at last stay quick to open.
const topPoolSize = 4

// topLoader collects the detail top shows of a job with the session's clients, the same way why and history
// do, through port-forward pools kept for the whole session.
type topLoader struct {
	o        *Options
	clients  map[string]*kube.Client
	profile  *profile.Profile
	rules    []*triage.Rule
	redactor *redact.Redactor

	mu    sync.Mutex
	pools map[string]*flink.Pool
}

func newTopLoader(o *Options, srcs []fleet.Source, prof *profile.Profile) (*topLoader, error) {
	rules, err := prof.VerdictRules(triage.DefaultRules())
	if err != nil {
		return nil, err
	}
	r, err := prof.Redactor()
	if err != nil {
		return nil, err
	}
	l := &topLoader{o: o, clients: map[string]*kube.Client{}, profile: prof, rules: rules, redactor: r, pools: map[string]*flink.Pool{}}
	for _, s := range srcs {
		if s.Err == nil {
			l.clients[s.Scope.Context] = s.Client
		}
	}
	return l, nil
}

func (l *topLoader) target(job *model.Job) (*target, error) {
	c := l.clients[job.Context]
	if c == nil {
		return nil, fmt.Errorf("context %s has no client", job.Context)
	}
	return &target{job: job, client: c, profile: l.profile}, nil
}

// pool is the context's port-forward pool, opened on first use.
func (l *topLoader) pool(t *target) *flink.Pool {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.pools[t.job.Context]
	if p == nil {
		p = newPool(t.client, topPoolSize)
		l.pools[t.job.Context] = p
	}
	return p
}

// Detail collects why's evidence and verdict and builds the history, as why and history do.
func (l *topLoader) Detail(ctx context.Context, job model.Job) (*tui.Detail, error) {
	t, err := l.target(&job)
	if err != nil {
		return nil, err
	}
	now := l.o.now()
	ev, raw := l.o.collectJob(ctx, t, l.pool(t), false)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	v, err := triage.Judge(ev, triage.Signals(ev), l.rules)
	if err != nil {
		return nil, err
	}
	d := &tui.Detail{Evidence: ev, Verdict: v, Loaded: now}
	// As history does: without the resource, the timeline still has the events and status.
	d.History, d.HistoryErr = history.Build(ev, raw, l.redactor)
	return d, nil
}

// Log reads the tail of the JobManager's log, of every JobManager pod with a header each when there are several.
func (l *topLoader) Log(ctx context.Context, job model.Job, tail int) (string, error) {
	t, err := l.target(&job)
	if err != nil {
		return "", err
	}
	pods, err := t.client.ComponentPods(ctx, job.Namespace, t.cluster(), kube.ComponentJM)
	if err != nil {
		return "", err
	}
	if len(pods) == 0 {
		return "", fmt.Errorf("%s has no JobManager pod", t)
	}
	var b strings.Builder
	for i := range pods {
		if len(pods) > 1 {
			fmt.Fprintf(&b, "==> %s <==\n", pods[i].Name)
		}
		opts := kube.LogOptions{Container: kube.MainContainer, TailLines: int64(tail)}
		rc, err := t.client.Logs(ctx, job.Namespace, pods[i].Name, opts)
		if err != nil && restarted(&pods[i]) {
			// A crash-looping JobManager has no current log to read; the one before the crash explains it.
			opts.Previous = true
			if rc, err = t.client.Logs(ctx, job.Namespace, pods[i].Name, opts); err == nil {
				fmt.Fprintf(&b, "==> %s, the container before its last restart <==\n", pods[i].Name)
			}
		}
		if err != nil {
			fmt.Fprintf(&b, "log of %s: %v\n", pods[i].Name, err)
			continue
		}
		_, err = io.Copy(&b, rc)
		_ = rc.Close()
		if err != nil {
			return b.String(), err
		}
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// restarted is true when the pod's main container has restarted, so that a previous log exists.
func restarted(p *kube.Pod) bool {
	for _, c := range p.Status.ContainerStatuses {
		if c.Name == kube.MainContainer && c.RestartCount > 0 {
			return true
		}
	}
	return false
}

// Report writes the job's handover bundle into the current directory, as kubectl flink report does.
func (l *topLoader) Report(ctx context.Context, job model.Job) (string, error) {
	t, err := l.target(&job)
	if err != nil {
		return "", err
	}
	b, ref, now, err := l.o.bundle(ctx, t, l.pool(t), false, l.rules, l.redactor,
		report.Options{Tail: report.DefaultTail, MaxTaskManagers: report.DefaultMaxTaskManagers})
	if err != nil {
		return "", err
	}
	return writeTarGz(b, ref, now, "")
}

func (l *topLoader) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.pools {
		p.Close()
	}
}
