package fleet

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/patrk/kubectl-flink/internal/model"
)

// Row is a job with its health at one moment.
type Row struct {
	Job *model.Job
	model.Assessment
}

// Rows assesses the jobs of every scope at now.
func Rows(scopes []Scope, now time.Time, th model.Thresholds) []Row {
	n := 0
	for i := range scopes {
		n += len(scopes[i].Jobs)
	}
	rows := make([]Row, 0, n)
	for i := range scopes {
		for j := range scopes[i].Jobs {
			job := &scopes[i].Jobs[j]
			rows = append(rows, Row{Job: job, Assessment: model.Assess(job, now, th)})
		}
	}
	return rows
}

// Unhealthy is true for the health values --unhealthy and --exit-code act on.
func Unhealthy(h model.Health) bool { return h >= model.Degraded }

// Filter keeps the rows a user asked for.
type Filter struct {
	// Health keeps only these values; empty keeps all.
	Health []model.Health
	// Text keeps rows whose namespace/name contains it, or that carry it as an ID label value.
	Text string
	// IDLabels are the label keys whose values identify a job; empty means every label.
	IDLabels []string
}

// Keep reports whether r passes the filter.
func (f *Filter) Keep(r *Row) bool {
	if len(f.Health) > 0 && !slices.Contains(f.Health, r.Health) {
		return false
	}
	return f.Text == "" || strings.Contains(r.Job.Key(), f.Text) || HasID(r.Job, f.IDLabels, f.Text)
}

// HasID reports whether one of the job's ID labels, on the resource or its pod template, has the value id.
// People name jobs by a business ID kept in a label more often than by the resource name. Values are compared
// exactly, so that a short word does not match every job whose labels contain it.
func HasID(j *model.Job, idLabels []string, id string) bool {
	for _, labels := range []map[string]string{j.Labels, j.PodLabels} {
		for k, v := range labels {
			if v == id && (len(idLabels) == 0 || slices.Contains(idLabels, k)) {
				return true
			}
		}
	}
	return false
}

// Apply returns the rows that pass.
func (f *Filter) Apply(rows []Row) []Row {
	out := rows[:0:0]
	for i := range rows {
		if f.Keep(&rows[i]) {
			out = append(out, rows[i])
		}
	}
	return out
}

// SortKeys are the accepted --sort values.
var SortKeys = []string{"health", "name", "namespace", "state-age", "age"}

// Sort orders rows by key. Every order ends with context, namespace and name, so output is stable. health puts
// the worst first; the ages put the oldest first.
func Sort(rows []Row, key string) error {
	var primary func(a, b *Row) int
	switch key {
	case "", "health":
		primary = func(a, b *Row) int { return cmp.Compare(b.Health, a.Health) }
	case "name":
		primary = func(a, b *Row) int { return cmp.Compare(a.Job.Name, b.Job.Name) }
	case "namespace":
		primary = func(*Row, *Row) int { return 0 }
	case "state-age":
		primary = func(a, b *Row) int { return a.Job.StateSince().Compare(b.Job.StateSince()) }
	case "age":
		primary = func(a, b *Row) int { return a.Job.Created.Compare(b.Job.Created) }
	default:
		return fmt.Errorf("unknown sort key %q, use one of %s", key, strings.Join(SortKeys, ", "))
	}
	slices.SortStableFunc(rows, func(a, b Row) int {
		return cmp.Or(primary(&a, &b),
			cmp.Compare(a.Job.Context, b.Job.Context),
			cmp.Compare(a.Job.Namespace, b.Job.Namespace),
			cmp.Compare(a.Job.Name, b.Job.Name))
	})
	return nil
}

// ErrNoMatch means no job matched a job argument.
var ErrNoMatch = errors.New("no job matches")

// AmbiguousError lists the jobs an argument matched when it should have matched one.
type AmbiguousError struct {
	Arg        string
	Candidates []*model.Job
}

func (e *AmbiguousError) Error() string {
	keys := make([]string, len(e.Candidates))
	for i, j := range e.Candidates {
		keys[i] = j.Key()
		if j.Context != "" {
			keys[i] = j.Context + ": " + keys[i]
		}
	}
	return fmt.Sprintf("%q matches %d jobs:\n  %s", e.Arg, len(keys), strings.Join(keys, "\n  "))
}

// Find resolves a job argument, trying in turn: namespace/name or a name exactly, an ID label value exactly
// (see HasID), and a unique substring of namespace/name. Several matches at one step are an AmbiguousError.
func Find(jobs []*model.Job, idLabels []string, arg string) (*model.Job, error) {
	exact := func(j *model.Job) bool {
		if strings.Contains(arg, "/") {
			return j.Key() == arg
		}
		return j.Name == arg
	}
	byID := func(j *model.Job) bool { return HasID(j, idLabels, arg) }
	substring := func(j *model.Job) bool { return strings.Contains(j.Key(), arg) }
	for _, pred := range []func(*model.Job) bool{exact, byID, substring} {
		var found []*model.Job
		for _, j := range jobs {
			if pred(j) {
				found = append(found, j)
			}
		}
		switch len(found) {
		case 0:
			continue
		case 1:
			return found[0], nil
		default:
			return nil, &AmbiguousError{Arg: arg, Candidates: found}
		}
	}
	return nil, fmt.Errorf("%w %q", ErrNoMatch, arg)
}
