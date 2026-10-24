package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/kube"
)

type logsOptions struct {
	*Options
	allNamespaces bool
	taskManagers  bool
	follow        bool
	previous      bool
	tail          int64
	since         time.Duration
}

func newLogsCommand(o *Options) *cobra.Command {
	lo := &logsOptions{Options: o}
	cmd := &cobra.Command{
		Use:   "logs JOB",
		Short: "Print the JobManager or TaskManager logs of a job",
		Long: `Print the log of the job's JobManager, or with --taskmanagers of every TaskManager, each line prefixed
with its pod. JOB is namespace/name, a name, an ID label value or a unique part of a name. A session job reads
its session cluster's pods. Use --previous after a crash.`,
		Example: `  kubectl flink logs team-a/orders --tail 100
  kubectl flink logs orders --previous
  kubectl flink logs orders --taskmanagers -f`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error { return lo.run(cmd.Context(), args[0]) },
	}
	f := cmd.Flags()
	f.BoolVarP(&lo.allNamespaces, "all-namespaces", "A", false, "Look for the job in all namespaces")
	f.BoolVar(&lo.taskManagers, "taskmanagers", false, "Print the TaskManager logs instead of the JobManager's")
	f.BoolVarP(&lo.follow, "follow", "f", false, "Keep streaming new lines")
	f.BoolVarP(&lo.previous, "previous", "p", false, "Print the log of the previous container, after a restart")
	f.Int64Var(&lo.tail, "tail", 200, "Lines from the end of each log; -1 for all")
	f.DurationVar(&lo.since, "since", 0, "Only lines newer than this, such as 10m")
	return cmd
}

func (lo *logsOptions) run(ctx context.Context, arg string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := lo.resolveJob(ctx, arg, lo.allNamespaces)
	if err != nil {
		return err
	}
	component := kube.ComponentJM
	if lo.taskManagers {
		component = kube.ComponentTM
	}
	pods, err := t.client.ComponentPods(ctx, t.job.Namespace, t.cluster(), component)
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		return fmt.Errorf("%s has no %s pods", t, component)
	}
	opts := kube.LogOptions{Container: kube.MainContainer, Follow: lo.follow, Previous: lo.previous, SinceSeconds: int64(lo.since.Seconds())}
	if lo.tail >= 0 {
		opts.TailLines = lo.tail
	}
	if len(pods) == 1 {
		return lo.copy(ctx, t, pods[0].Name, opts, "")
	}
	// Several pods are streamed together, line by line, each prefixed with its pod. One pod failing, such as one
	// without a previous container, is reported and does not cut off the others.
	var wg sync.WaitGroup
	errs := make([]error, len(pods))
	for i := range pods {
		name := pods[i].Name
		wg.Go(func() { errs[i] = lo.copy(ctx, t, name, opts, "["+name+"] ") })
	}
	wg.Wait()
	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
			outMu.Lock()
			_, _ = fmt.Fprintf(lo.ErrOut, "warning: %v\n", err)
			outMu.Unlock()
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d logs could not be read", failed, len(pods))
	}
	return nil
}

var outMu sync.Mutex

func (lo *logsOptions) copy(ctx context.Context, t *target, pod string, opts kube.LogOptions, prefix string) error {
	rc, err := t.client.Logs(ctx, t.job.Namespace, pod, opts)
	if err != nil {
		return fmt.Errorf("log of %s: %w", pod, err)
	}
	defer rc.Close()
	if prefix == "" {
		_, err = io.Copy(lo.Out, rc)
		return err
	}
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		outMu.Lock()
		_, err := fmt.Fprintf(lo.Out, "%s%s\n", prefix, sc.Bytes())
		outMu.Unlock()
		if err != nil {
			return err
		}
	}
	return sc.Err()
}
