package triage

import (
	"cmp"
	"slices"

	"github.com/patrk/kubectl-flink/internal/model"
)

// Verdict is why's answer: the health, the known patterns that explain it, and, when none does, the signals
// most likely to.
type Verdict struct {
	Health   model.Health `json:"health"`
	Findings []Finding    `json:"findings"`
	// Leads are the strongest generic signals, given when the job is unhealthy and no rule explains it.
	Leads []Signal `json:"leads,omitempty"`
}

// rankLayers orders findings of equal severity: a Kubernetes cause usually explains an operator state, which
// usually explains what the runtime reports.
var rankLayers = []Layer{Kubernetes, Operator, Runtime}

// leadKinds are the generic signals that best point at a cause, in order of preference.
// The health findings come last: they always exist for an unhealthy job, so there is always a lead.
var leadKinds = []string{SigExceptionCause, SigException, SigPodTerminated, SigPodWaiting, SigPodUnschedulable, SigLog, SigEvent, SigOperatorError, SigHealth}

// maxLeads bounds the leads, to keep why on one screen.
const maxLeads = 4

// Judge matches signals against rules and builds the verdict for ev.
func Judge(ev *Evidence, signals []Signal, rules []*Rule) (*Verdict, error) {
	findings, err := Evaluate(rules, signals, ev.Job)
	if err != nil {
		return nil, err
	}
	superseded := map[string]bool{}
	for _, f := range findings {
		for _, id := range f.rule.Supersedes {
			superseded[id] = true
		}
	}
	findings = slices.DeleteFunc(findings, func(f Finding) bool { return superseded[f.ID] })
	slices.SortStableFunc(findings, func(a, b Finding) int {
		return cmp.Or(cmp.Compare(b.Severity, a.Severity),
			cmp.Compare(slices.Index(rankLayers, a.Layer), slices.Index(rankLayers, b.Layer)))
	})
	v := &Verdict{Health: ev.Health.Health, Findings: findings}
	if v.Findings == nil {
		v.Findings = []Finding{}
	}
	explained := slices.ContainsFunc(findings, func(f Finding) bool { return f.Severity >= model.Degraded })
	if ev.Health.Health >= model.Degraded && !explained {
		v.Leads = leads(signals)
	}
	return v, nil
}

func leads(signals []Signal) []Signal {
	var out []Signal
	for _, kind := range leadKinds {
		for _, s := range signals {
			if s.Kind == kind {
				out = append(out, s)
				break
			}
		}
		if len(out) == maxLeads {
			break
		}
	}
	return out
}
