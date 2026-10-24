package cli

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

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

// ExceptionReport is the -o json and -o yaml document of exceptions.
type ExceptionReport struct {
	Job       render.JobRef     `json:"job"`
	Entries   []flink.Exception `json:"entries"`
	Truncated bool              `json:"truncated,omitempty"`
}

func (eo *exceptionsOptions) run(ctx context.Context, t *target, c *flink.Client) error {
	ex, err := c.Exceptions(ctx, t.job.JobID, eo.limit)
	if err != nil {
		return err
	}
	rep := ExceptionReport{Job: render.NewJobRef(t.job), Entries: ex.History.Entries, Truncated: ex.History.Truncated}
	if rep.Entries == nil {
		rep.Entries = []flink.Exception{}
	}
	switch eo.output {
	case "json":
		return render.JSON(eo.Out, rep)
	case "yaml":
		return render.YAML(eo.Out, rep)
	}
	return printExceptions(eo.Out, &rep, eo.full, eo.now())
}

// shortClass drops the package from "org.example.FooException: message".
func shortClass(s string) string {
	cls, msg, found := strings.Cut(s, ": ")
	if i := strings.LastIndex(cls, "."); i >= 0 && !strings.Contains(cls, " ") {
		cls = cls[i+1:]
	}
	if found {
		return cls + ": " + msg
	}
	return cls
}

func printExceptions(out io.Writer, rep *ExceptionReport, full bool, now time.Time) error {
	w := &errWriter{w: out}
	w.printf("Job: %s\n", rep.Job)
	if len(rep.Entries) == 0 {
		w.printf("No failures since the job started.\n")
		return w.err
	}
	more := ""
	if rep.Truncated {
		more = ", older ones truncated"
	}
	noun := "failures"
	if len(rep.Entries) == 1 {
		noun = "failure"
	}
	w.printf("%d %s, newest first%s\n", len(rep.Entries), noun, more)
	for i := range rep.Entries {
		e := &rep.Entries[i]
		w.printf("\n%s  %s\n", ago(e.Timestamp.Time(), now), shortClass(e.Message()))
		if cause := e.Cause(); cause != "" {
			w.printf("  caused by: %s\n", shortClass(cause))
		}
		if e.TaskName != "" {
			w.printf("  task: %s\n", e.TaskName)
		}
		if where := e.Where(); where != "" {
			w.printf("  on: %s\n", where)
		}
		for _, k := range slices.Sorted(maps.Keys(e.FailureLabels)) {
			w.printf("  label %s: %s\n", k, e.FailureLabels[k])
		}
		if n := len(e.Concurrent); n > 0 {
			w.printf("  with %d concurrent failures\n", n)
		}
		if full {
			w.printf("\n%s\n", indent(strings.TrimSpace(e.Stacktrace), "    "))
		}
	}
	return w.err
}

func indent(s, prefix string) string { return prefix + strings.ReplaceAll(s, "\n", "\n"+prefix) }

// errWriter keeps the first write error, so printing code stays linear.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err == nil {
		_, e.err = fmt.Fprintf(e.w, format, args...)
	}
}
