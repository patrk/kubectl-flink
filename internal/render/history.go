package render

import (
	"fmt"
	"io"
	"time"

	"github.com/liggitt/tabwriter"

	"github.com/patrk/kubectl-flink/internal/history"
)

// HistoryReport is the -o json and -o yaml document of history.
type HistoryReport struct {
	Job JobRef `json:"job"`
	*history.History
}

// History prints the timeline newest first, then the spec changes the operator has not applied.
func History(out io.Writer, job JobRef, h *history.History, now time.Time) error {
	return HistoryWidth(out, job, h, now, 0)
}

// HistoryWidth is History with lines cut to width, for a terminal pane; 0 is the default width.
func HistoryWidth(out io.Writer, job JobRef, h *history.History, now time.Time, width int) error {
	w := &lineWriter{w: out, width: width}
	w.line(0, "%s", job)
	w.blank()
	if err := w.err; err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TIME (UTC)\tAGO\tSOURCE\tWHAT")
	for _, e := range h.Entries {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", e.At.UTC().Format("2006-01-02 15:04:05"), age(e.At, now), e.Source, truncate(e.Text, 110))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if len(h.Pending) > 0 {
		w.blank()
		w.line(0, "Not applied yet: the spec differs from what the operator last deployed")
		for _, c := range h.Pending {
			w.line(1, "%s: %s -> %s", c.Field, dash(c.From), dash(c.To))
		}
	}
	w.blank()
	w.wrap(0, h.Horizon)
	return w.err
}
