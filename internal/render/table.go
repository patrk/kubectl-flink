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
	// ReasonWidth truncates the reason; 0 is 60, or 120 when Wide.
	ReasonWidth int
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
	t.ageIdx = slices.Index(o.Header(), ageColumn)
	return t
}

var healthColors = map[model.Health]string{
	model.OK: "32", model.Suspended: "2", model.Progressing: "36", model.Degraded: "33", model.Stuck: "35", model.Failed: "31",
}

const (
	// ErrorHealth stands in the health column of a scope that could not be read.
	ErrorHealth = "ERROR"
	ageColumn   = "IN STATE"
	// HealthColumn is the header of the health column, which callers colour themselves when Color is off.
	HealthColumn = "HEALTH"
)

// Header is the column names.
func (o TableOptions) Header() []string {
	var h []string
	if o.ShowContext {
		h = append(h, "CONTEXT")
	}
	if o.ShowNamespace {
		h = append(h, "NAMESPACE")
	}
	h = append(h, "NAME", HealthColumn, "LIFECYCLE", "JOB", "JM", ageColumn, "REASON")
	if o.Wide {
		h = append(h, "TM", "JM RESTARTS", "DRIFT", "FLINK", "IMAGE", "OWNER", "SNAPSHOT AGE")
	}
	return h
}

// colored wraps s in an ANSI colour that the tabwriter does not count towards the column width.
func (o TableOptions) colored(s, code string) string {
	if !o.Color || code == "" {
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

// Cells is the row's cells at now, in the order of Header.
func (o TableOptions) Cells(r *fleet.Row, now time.Time) []string {
	j := r.Job
	var cells []string
	if o.ShowContext {
		cells = append(cells, j.Context)
	}
	if o.ShowNamespace {
		cells = append(cells, j.Namespace)
	}
	reason, width := "-", 60
	if o.Wide {
		width = 120
	}
	if o.ReasonWidth > 0 {
		width = o.ReasonWidth
	}
	if len(r.Findings) > 0 {
		reason = truncate(r.Findings[0].Text(now), width)
	}
	age := "-"
	if since := j.StateSince(); !since.IsZero() {
		age = duration.HumanDuration(now.Sub(since))
	}
	cells = append(cells, j.Name, o.colored(r.Health.String(), healthColors[r.Health]),
		dash(j.Lifecycle), dash(j.JobState), dash(j.JMStatus), age, reason)
	if o.Wide {
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
		snapshot := "-"
		if !j.LastSnapshot.IsZero() {
			snapshot = duration.HumanDuration(now.Sub(j.LastSnapshot))
		}
		cells = append(cells, tm, restarts, drift, dash(flinkVersion(j)), dash(j.Image), dash(strings.Join(j.SpecOwners, ",")), snapshot)
	}
	return cells
}

// cells returns the row's cells and its change signature, which leaves out the age so that time passing alone
// does not reprint a row.
func (t *Table) cells(r *fleet.Row, now time.Time) (cells []string, sig string) {
	cells = t.o.Cells(r, now)
	j := r.Job
	// The signature leaves out everything that changes with time alone: the ages, and the duration in the reason.
	stable := slices.Clone(cells)
	stable[t.ageIdx] = ""
	if len(r.Findings) > 0 {
		stable[t.ageIdx+1] = r.Findings[0].Reason
	}
	if t.o.Wide {
		stable[len(stable)-1] = j.LastSnapshot.String()
	}
	return cells, strings.Join(stable, "\t")
}

func (t *Table) errorCells(s *fleet.Scope) []string { return t.o.ErrorCells(s) }

// ErrorCells is the row of a scope that could not be read, in the order of Header.
func (o TableOptions) ErrorCells(s *fleet.Scope) []string {
	var cells []string
	if o.ShowContext {
		cells = append(cells, s.Context)
	}
	if o.ShowNamespace {
		cells = append(cells, "-")
	}
	cells = append(cells, "-", o.colored(ErrorHealth, "31"), "-", "-", "-", "-", truncate(s.Err.Error(), 120))
	if o.Wide {
		cells = append(cells, "-", "-", "-", "-", "-", "-", "-")
	}
	return cells
}

func (t *Table) line(cells []string) {
	_, _ = io.WriteString(t.w, strings.Join(cells, "\t")+"\n")
}

// Print prints the header, an error row per failed scope and every row.
func (t *Table) Print(rows []fleet.Row, scopes []fleet.Scope, now time.Time) error {
	if !t.o.NoHeaders {
		t.line(t.o.Header())
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
		cells = append(cells, "-", "-", "-", "-", "-", "-", "-")
	}
	return cells
}

func key(j *model.Job) string { return Key(j) }

// Key identifies a job across contexts.
func Key(j *model.Job) string { return j.Context + "\x00" + j.Key() }
