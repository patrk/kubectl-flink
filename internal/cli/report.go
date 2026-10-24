package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/history"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/report"
	"github.com/patrk/kubectl-flink/internal/triage"
	"github.com/patrk/kubectl-flink/internal/version"
)

type reportOptions struct {
	*Options
	allNamespaces   bool
	restURL         string
	dir             string
	file            string
	tail            int64
	maxTaskManagers int
	quiet           bool
}

func newReportCommand(o *Options) *cobra.Command {
	ro := &reportOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "report JOB",
		Short: "Write a debugging handover for a job, secrets redacted",
		Long: `Collect everything known about a job into one bundle: the resource, pods, events, the rendered Flink
configuration, JobManager and TaskManager logs, the JobManager's REST answers, why and history. Secrets are
redacted. summary.md, printed on stdout, is the part to paste into a chat; the bundle holds the rest. JOB is
namespace/name, a name, an ID label value or a unique part of a name.`,
		Example: `  kubectl flink report team-a/orders
  kubectl flink report orders --dir ./orders-report
  kubectl flink report orders --tail 500 --max-taskmanagers 2 -q`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if ro.tail < 1 || ro.maxTaskManagers < 0 {
				return &exitError{code: ExitUsage, err: fmt.Errorf("--tail must be at least 1 and --max-taskmanagers at least 0")}
			}
			if ro.dir != "" && ro.file != "" {
				return &exitError{code: ExitUsage, err: fmt.Errorf("--dir and --file exclude each other")}
			}
			return ro.run(cmd.Context(), args[0])
		},
	}
	f := cmd.Flags()
	f.BoolVarP(&ro.allNamespaces, "all-namespaces", "A", false, "Look for the job in all namespaces")
	f.StringVar(&ro.restURL, "rest-url", "", "JobManager REST URL to use instead of a port-forward")
	f.StringVar(&ro.dir, "dir", "", "Write the bundle into this directory instead of a .tar.gz")
	f.StringVar(&ro.file, "file", "", "Path of the .tar.gz; default kubectl-flink-report-<namespace>-<name>-<time>.tar.gz")
	f.Int64Var(&ro.tail, "tail", report.DefaultTail, "Log lines per container")
	f.IntVar(&ro.maxTaskManagers, "max-taskmanagers", report.DefaultMaxTaskManagers, "TaskManagers whose logs are included, those with trouble first")
	f.BoolVarP(&ro.quiet, "quiet", "q", false, "Print only the path of the bundle")
	return cmd
}

func (ro *reportOptions) run(ctx context.Context, arg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := ro.resolveJob(ctx, arg, ro.allNamespaces)
	if err != nil {
		return err
	}
	r, err := t.profile.Redactor()
	if err != nil {
		return err
	}
	rules, err := t.profile.VerdictRules(triage.DefaultRules())
	if err != nil {
		return err
	}
	ev, raw, access, closeFn := ro.collectJob(ctx, t, ro.restURL)
	defer closeFn()
	signals := triage.Signals(ev)
	v, err := triage.Judge(ev, signals, rules)
	if err != nil {
		return err
	}
	h, err := history.Build(ev, raw, r)
	if err != nil {
		return err
	}
	now := ro.now()

	bundle := report.Collect(ctx, report.Source{Kube: t.client, REST: access}, ev.Job, raw, ev.Pods, ev.Events, r,
		report.Options{Tail: ro.tail, MaxTaskManagers: ro.maxTaskManagers})
	var why, hist bytes.Buffer
	if err := render.Why(&why, ev, v); err != nil {
		return err
	}
	ref := render.NewJobRef(ev.Job)
	if err := render.History(&hist, ref, h, now); err != nil {
		return err
	}
	bundle.Add(r, "why.txt", why.String())
	bundle.Add(r, "history.txt", hist.String())
	reasons := make([]string, 0, len(ev.Health.Findings))
	for i := range ev.Health.Findings {
		reasons = append(reasons, ev.Health.Findings[i].Text(now))
	}
	for _, g := range ev.Gaps {
		bundle.Gaps = append(bundle.Gaps, g.What+": "+g.Why)
	}
	slices.Sort(bundle.Gaps)
	bundle.Gaps = slices.Compact(bundle.Gaps)
	slices.SortFunc(bundle.Files, func(a, b report.File) int { return strings.Compare(a.Name, b.Name) })
	summary := bundle.Summary(&report.SummaryInput{
		Job: ref, Health: ev.Health.Health.String(), Reasons: reasons, Verdict: v, History: h, Captured: now, Version: version.Version,
	})
	bundle.Add(r, "summary.md", summary)
	slices.SortFunc(bundle.Files, func(a, b report.File) int { return strings.Compare(a.Name, b.Name) })

	path, err := ro.write(bundle, ref, now)
	if err != nil {
		return err
	}
	if !ro.quiet {
		for _, f := range bundle.Files {
			if f.Name == "summary.md" {
				_, _ = ro.Out.Write(f.Data)
			}
		}
	}
	_, _ = fmt.Fprintf(ro.ErrOut, "wrote %s (%d files, %d KiB uncompressed)\n", path, len(bundle.Files), bundle.Size()/1024)
	if ro.quiet {
		_, _ = fmt.Fprintln(ro.Out, path)
	}
	return nil
}

// write puts the bundle into --dir, or into a .tar.gz named after the job and the capture time.
func (ro *reportOptions) write(b *report.Bundle, ref render.JobRef, now time.Time) (string, error) {
	if ro.dir != "" {
		return ro.dir, b.WriteDir(ro.dir)
	}
	top := fmt.Sprintf("kubectl-flink-report-%s-%s-%s", ref.Namespace, ref.Name, now.UTC().Format("20060102T150405Z"))
	path := ro.file
	if path == "" {
		path = top + ".tar.gz"
	}
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	if err := b.WriteTarGz(f, top, now); err != nil {
		_ = f.Close()
		_ = os.Remove(path) // never leave a truncated bundle behind
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}
