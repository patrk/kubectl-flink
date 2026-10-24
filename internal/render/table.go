package render

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/liggitt/tabwriter"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

// TableOptions shape the table.
type TableOptions struct {
	Wide          bool
	NoHeaders     bool
	Color         bool
	ShowContext   bool
	ShowNamespace bool
}

// Table prints rows as a table. With Watch it prints the header and all rows once, then only the rows whose
// content changed, keeping the column widths of the first print, as kubectl get -w does.
type Table struct {
	w      *tabwriter.Writer
	o      TableOptions
	ageIdx int
	last   map[string]string // row key → change signature of the last printed version
}

// NewTable returns a table printer writing to out.
func NewTable(out io.Writer, o TableOptions) *Table {
	t := &Table{
		w:    tabwriter.NewWriter(out, 0, 8, 3, ' ', tabwriter.StripEscape|tabwriter.RememberWidths),
		o:    o,
		last: map[string]string{},
	}
	t.ageIdx = slices.Index(t.header(), ageColumn)
	return t
}

var healthColors = map[model.Health]string{
	model.OK: "32", model.Suspended: "2", model.Progressing: "36", model.Degraded: "33", model.Stuck: "35", model.Failed: "31",
}

const (
	errorHealth = "ERROR"
	ageColumn   = "IN STATE"
)

func (t *Table) header() []string {
	var h []string
	if t.o.ShowContext {
		h = append(h, "CONTEXT")
	}
	if t.o.ShowNamespace {
		h = append(h, "NAMESPACE")
	}
	h = append(h, "NAME", "HEALTH", "LIFECYCLE", "JOB", "JM", ageColumn, "REASON")
	if t.o.Wide {
		h = append(h, "TM", "JM RESTARTS", "DRIFT", "FLINK", "IMAGE", "OWNER")
	}
	return h
}

// colored wraps s in an ANSI colour that the tabwriter does not count towards the column width.
func (t *Table) colored(s, code string) string {
	if !t.o.Color || code == "" {
		return s
	}
	return "\xff\x1b[" + code + "m\xff" + s + "\xff\x1b[0m\xff"
}

func dash(s string) string { return cmp.Or(s, "-") }

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// cells returns the row's cells and its change signature, which leaves out the age so that time passing alone
// does not reprint a row.
func (t *Table) cells(r *fleet.Row, now time.Time) (cells []string, sig string) {
	j := r.Job
	if t.o.ShowContext {
		cells = append(cells, j.Context)
	}
	if t.o.ShowNamespace {
		cells = append(cells, j.Namespace)
	}
	reason, width := "-", 60
	if t.o.Wide {
		width = 120
	}
	if len(r.Findings) > 0 {
		reason = truncate(r.Findings[0].Text(now), width)
	}
	age := "-"
	if since := j.StateSince(); !since.IsZero() {
		age = duration.HumanDuration(now.Sub(since))
	}
	cells = append(cells, j.Name, t.colored(r.Health.String(), healthColors[r.Health]),
		dash(j.Lifecycle), dash(j.JobState), dash(j.JMStatus), age, reason)
	if t.o.Wide {
		tm, restarts := "-", "-"
		if j.Pods != nil {
			restarts = strconv.Itoa(int(j.Pods.JM.Restarts))
			if j.Kind == model.KindDeployment && !j.SessionCluster {
				tm = fmt.Sprintf("%d/%d", j.Pods.TM.Ready, j.TMReplicas)
			}
		}
		drift := "-"
		if j.Drift() {
			drift = fmt.Sprintf("%d>%d", j.ObservedGeneration, j.Generation)
		}
		cells = append(cells, tm, restarts, drift, dash(flinkVersion(j)), dash(j.Image), dash(strings.Join(j.SpecOwners, ",")))
	}
	// The signature leaves out everything that changes with time alone: the age, and the duration in the reason.
	stable := slices.Clone(cells)
	stable[t.ageIdx] = ""
	if len(r.Findings) > 0 {
		stable[t.ageIdx+1] = r.Findings[0].Reason
	}
	return cells, strings.Join(stable, "\t")
}

func (t *Table) errorCells(s *fleet.Scope) []string {
	cells := []string{s.Context}
	if t.o.ShowNamespace {
		cells = append(cells, "-")
	}
	cells = append(cells, "-", t.colored(errorHealth, "31"), "-", "-", "-", "-", truncate(s.Err.Error(), 120))
	if t.o.Wide {
		cells = append(cells, "-", "-", "-", "-", "-", "-")
	}
	return cells
}

func (t *Table) line(cells []string) {
	_, _ = io.WriteString(t.w, strings.Join(cells, "\t")+"\n")
}

// Print prints the header, an error row per failed scope and every row.
func (t *Table) Print(rows []fleet.Row, scopes []fleet.Scope, now time.Time) error {
	if !t.o.NoHeaders {
		t.line(t.header())
	}
	for i := range scopes {
		if s := &scopes[i]; s.Err != nil && t.o.ShowContext {
			t.line(t.errorCells(s))
			t.last["scope:"+s.Context] = s.Err.Error()
		}
	}
	for i := range rows {
		cells, sig := t.cells(&rows[i], now)
		t.line(cells)
		t.last[key(rows[i].Job)] = sig
	}
	return t.w.Flush()
}

// Update prints the rows and scope errors that changed since the last print, and a row for each job that is gone.
func (t *Table) Update(rows []fleet.Row, scopes []fleet.Scope, now time.Time) error {
	seen := make(map[string]bool, len(rows))
	for i := range scopes {
		s := &scopes[i]
		k := "scope:" + s.Context
		seen[k] = true
		if s.Err == nil {
			delete(t.last, k)
			continue
		}
		if t.last[k] != s.Err.Error() && t.o.ShowContext {
			t.line(t.errorCells(s))
			t.last[k] = s.Err.Error()
		}
	}
	for i := range rows {
		k := key(rows[i].Job)
		seen[k] = true
		cells, sig := t.cells(&rows[i], now)
		if t.last[k] != sig {
			t.line(cells)
			t.last[k] = sig
		}
	}
	for k := range t.last {
		if !seen[k] {
			t.line(t.goneCells(k))
			delete(t.last, k)
		}
	}
	return t.w.Flush()
}

func (t *Table) goneCells(k string) []string {
	ctx, rest, _ := strings.Cut(k, "\x00")
	ns, name, _ := strings.Cut(rest, "/")
	var cells []string
	if t.o.ShowContext {
		cells = append(cells, ctx)
	}
	if t.o.ShowNamespace {
		cells = append(cells, ns)
	}
	cells = append(cells, name, "-", "-", "-", "-", "-", "deleted")
	if t.o.Wide {
		cells = append(cells, "-", "-", "-", "-", "-", "-")
	}
	return cells
}

func key(j *model.Job) string { return j.Context + "\x00" + j.Key() }
