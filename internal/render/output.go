package render

import (
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"

	"github.com/patrk/kubectl-flink/internal/fleet"
)

// Formats are the accepted -o values.
var Formats = []string{"table", "wide", "json", "yaml", "name"}

// JSON writes v indented.
func JSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// YAML writes v as YAML, through its JSON form so that both outputs use the same field names.
func YAML(out io.Writer, v any) error {
	b, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	_, err = out.Write(b)
	return err
}

// Names writes namespace/name per row, the form job arguments accept.
func Names(out io.Writer, rows []fleet.Row) error {
	for i := range rows {
		if _, err := fmt.Fprintln(out, rows[i].Job.Key()); err != nil {
			return err
		}
	}
	return nil
}

// Event is one change in -o json -w and -o yaml -w output: a job (ADDED, MODIFIED, DELETED) or how a scope is
// read (SCOPE), sent when its note, errors or unread pods change.
type Event struct {
	Type  string       `json:"type"`
	Item  *Item        `json:"item,omitempty"`
	Scope *ScopeStatus `json:"scope,omitempty"`
}

// Events prints watch changes as JSON lines or YAML documents: every job once, then each job whose content
// changes, and each job that is gone.
type Events struct {
	out    io.Writer
	yaml   bool
	last   map[string]Item
	sig    map[string]string
	scopes map[string]string
}

// NewEvents returns an event printer; asYAML selects YAML documents instead of JSON lines.
func NewEvents(out io.Writer, asYAML bool) *Events {
	return &Events{out: out, yaml: asYAML, last: map[string]Item{}, sig: map[string]string{}, scopes: map[string]string{}}
}

func (e *Events) write(ev Event) error {
	if e.yaml {
		if _, err := io.WriteString(e.out, "---\n"); err != nil {
			return err
		}
		return YAML(e.out, ev)
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(e.out, "%s\n", b)
	return err
}

// Update prints the events since the last call. A scope is reported the first time only when it has something
// to say.
func (e *Events) Update(rows []fleet.Row, scopes []fleet.Scope) error {
	for i := range scopes {
		st := NewScopeStatus(&scopes[i])
		b, err := json.Marshal(st)
		if err != nil {
			return err
		}
		prev, known := e.scopes[st.Context]
		e.scopes[st.Context] = string(b)
		quiet := st.Note == "" && st.Error == "" && st.WatchError == "" && len(st.PodsUnread) == 0
		if prev == string(b) || !known && quiet {
			continue
		}
		if err := e.write(Event{Type: "SCOPE", Scope: &st}); err != nil {
			return err
		}
	}
	seen := make(map[string]bool, len(rows))
	for i := range rows {
		it := NewItem(&rows[i])
		k := key(rows[i].Job)
		seen[k] = true
		b, err := json.Marshal(it)
		if err != nil {
			return err
		}
		typ := "MODIFIED"
		if _, ok := e.sig[k]; !ok {
			typ = "ADDED"
		} else if e.sig[k] == string(b) {
			continue
		}
		e.sig[k], e.last[k] = string(b), it
		if err := e.write(Event{Type: typ, Item: &it}); err != nil {
			return err
		}
	}
	for k := range e.last {
		if !seen[k] {
			it := e.last[k]
			delete(e.last, k)
			delete(e.sig, k)
			if err := e.write(Event{Type: "DELETED", Item: &it}); err != nil {
				return err
			}
		}
	}
	return nil
}
