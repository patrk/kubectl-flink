// Package history reconstructs what happened to a job from what the cluster still remembers: events (kept
// about an hour), who wrote the spec and when (managedFields), status timestamps, container terminations, and
// the JobManager's failures and restores. It also lists spec changes the operator has not applied yet.
package history

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/redact"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// Entry is one moment in the job's history.
type Entry struct {
	At     time.Time `json:"at"`
	Source string    `json:"source"`
	Text   string    `json:"text"`
}

// Change is a spec field whose value differs between two versions of the spec.
type Change struct {
	Field string `json:"field"`
	From  string `json:"from"`
	To    string `json:"to"`
}

// History is a job's timeline, newest first, and its spec changes the operator has not applied.
type History struct {
	Entries []Entry `json:"entries"`
	// Pending are differences between the spec and the last spec the operator reconciled.
	Pending []Change `json:"pending,omitempty"`
	// Horizon says how far back each source reaches.
	Horizon string `json:"horizon"`
}

// Horizon is printed with every history, so that a quiet hour is not mistaken for nothing having happened.
const Horizon = "Events are kept for about an hour. Older changes show only as the last write of each field manager " +
	"(which moves with any field it writes), status timestamps and the JobManager's failure history."

// Build reconstructs the history from the evidence why collects and the raw resource, which holds the last
// reconciled spec. Secret values in spec changes are masked with r.
func Build(ev *triage.Evidence, rawCR []byte, r *redact.Redactor) (*History, error) {
	h := &History{Horizon: Horizon}
	add := func(at time.Time, source, text string) {
		if !at.IsZero() && text != "" {
			h.Entries = append(h.Entries, Entry{At: at, Source: source, Text: flatten(text)})
		}
	}
	j := ev.Job
	add(j.Created, "resource", "created, generation 1")
	add(j.DeletingSince, "resource", "deletion requested")
	for _, w := range j.SpecWriters {
		add(w.Time, "managedFields", fmt.Sprintf("%s last wrote the resource; it owns spec %s", w.Manager, strings.Join(w.Fields, ", ")))
	}
	add(j.ReconciledAt, "status", fmt.Sprintf("operator reconciled generation %d: lifecycle %s", j.ObservedGeneration, j.Lifecycle))
	add(j.JobStarted, "status", "job started")
	if j.JobStateSince.After(j.JobStarted) {
		add(j.JobStateSince, "status", "job state "+j.JobState)
	}
	for i := range ev.Events {
		e := &ev.Events[i]
		text := e.Reason + ": " + e.Message
		if n := e.Times(); n > 1 {
			// Events of the newer API have no firstTimestamp, only eventTime for the first occurrence.
			first := e.FirstTimestamp.Time
			if first.IsZero() {
				first = e.EventTime.Time
			}
			if first.IsZero() {
				text = fmt.Sprintf("%s (×%d)", text, n)
			} else {
				text = fmt.Sprintf("%s (×%d since %s)", text, n, first.UTC().Format(time.TimeOnly))
			}
		}
		add(e.Last(), "event "+strings.ToLower(e.InvolvedObject.Kind), text)
	}
	for i := range ev.Pods {
		p := &ev.Pods[i]
		component := p.Labels[kube.LabelComponent]
		add(p.CreationTimestamp.Time, "pod", fmt.Sprintf("%s pod %s created", component, p.Name))
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.LastState.Terminated; t != nil {
				add(t.FinishedAt.Time, "pod", fmt.Sprintf("%s %s exited: %s exit %d", component, p.Name, t.Reason, t.ExitCode))
			}
		}
	}
	if rs := ev.REST; rs != nil {
		for i := range rs.Exceptions {
			e := &rs.Exceptions[i]
			add(e.Timestamp.Time(), "job failure", e.Message())
		}
		if cps := rs.Checkpoints; cps != nil && cps.Latest.Restored != nil {
			kind := "checkpoint"
			if cps.Latest.Restored.IsSavepoint {
				kind = "savepoint"
			}
			add(cps.Latest.Restored.RestoredAt.Time(), "checkpoints", fmt.Sprintf("restored from %s #%d", kind, cps.Latest.Restored.ID))
		}
	}
	sort.SliceStable(h.Entries, func(a, b int) bool { return h.Entries[a].At.After(h.Entries[b].At) })

	if len(rawCR) > 0 {
		pending, err := pendingChanges(rawCR, r, j.Drift())
		if err != nil {
			return h, err
		}
		h.Pending = pending
	}
	return h, nil
}

func flatten(s string) string { return strings.Join(strings.Fields(s), " ") }

// pendingChanges compares spec with status.reconciliationStatus.lastReconciledSpec, the copy the operator keeps of
// what it last deployed.
// A field the spec leaves out but the last reconciled spec has is usually a default the operator filled in, so
// it counts as a change only while the operator has not observed the newest generation (drift).
func pendingChanges(raw []byte, r *redact.Redactor, drift bool) ([]Change, error) {
	var cr struct {
		Spec   map[string]any `json:"spec"`
		Status struct {
			ReconciliationStatus struct {
				LastReconciledSpec string `json:"lastReconciledSpec"`
			} `json:"reconciliationStatus"`
		} `json:"status"`
	}
	if err := decode(raw, &cr); err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}
	if cr.Status.ReconciliationStatus.LastReconciledSpec == "" {
		return nil, nil
	}
	var last struct {
		Spec map[string]any `json:"spec"`
	}
	if err := decode([]byte(cr.Status.ReconciliationStatus.LastReconciledSpec), &last); err != nil {
		return nil, fmt.Errorf("lastReconciledSpec: %w", err)
	}
	// Redacted as whole documents first: whether a value is secret can depend on its neighbours, such as the
	// argument after --password, which the flattened key does not show.
	spec, err := redacted(r, cr.Spec)
	if err != nil {
		return nil, err
	}
	lastSpec, err := redacted(r, last.Spec)
	if err != nil {
		return nil, err
	}
	now, before := map[string]string{}, map[string]string{}
	flattenInto("", spec, now)
	flattenInto("", lastSpec, before)
	keys := make([]string, 0, len(now)+len(before))
	for k := range now {
		keys = append(keys, k)
	}
	for k := range before {
		if _, ok := now[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	var out []Change
	for _, k := range keys {
		// The operator's copy writes unset fields as null where the spec leaves them out: both mean unset.
		from, to := unset(before[k]), unset(now[k])
		if from == to || to == "" && !drift {
			continue
		}
		out = append(out, Change{Field: k, From: from, To: to})
	}
	return out, nil
}

func unset(s string) string {
	if s == "null" {
		return ""
	}
	return s
}

func redacted(r *redact.Redactor, spec map[string]any) (any, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	if b, err = r.JSON(b); err != nil {
		return nil, err
	}
	r.Count()
	var v any
	err = decode(b, &v)
	return v, err
}

func decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	return d.Decode(v)
}

// flattenInto writes v as dotted keys; list items get their index, and a container's name when it has one.
func flattenInto(prefix string, v any, out map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			flattenInto(key, child, out)
		}
	case []any:
		for i, child := range t {
			label := fmt.Sprint(i)
			if m, ok := child.(map[string]any); ok {
				if name, ok := m["name"].(string); ok {
					label = name
				}
			}
			flattenInto(fmt.Sprintf("%s[%s]", prefix, label), child, out)
		}
	case nil:
		out[prefix] = "null"
	default:
		out[prefix] = fmt.Sprint(t)
	}
}
