package triage

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/patrk/kubectl-flink/internal/model"
)

// Verdict is why's answer: the health, the known patterns that match the evidence, and the strong signals no
// pattern accounts for. A pattern matching never hides the rest: rules know some failures, not all of them.
type Verdict struct {
	Health   model.Health `json:"health"`
	Findings []Finding    `json:"findings"`
	// Explained is true when a finding of DEGRADED or worse accounts for an unhealthy job.
	Explained bool `json:"explained"`
	// Unexplained are the strongest signals no finding used. For an unhealthy job nothing explains they are
	// where to look first; otherwise they are what else was seen.
	Unexplained []Signal `json:"unexplained,omitempty"`
}

// rankLayers orders findings of equal severity: a Kubernetes cause usually explains an operator state, which
// usually explains what the runtime reports; stale HA state explains a runtime surprise; performance comes last.
var rankLayers = []Layer{Kubernetes, Operator, HA, Runtime, Performance}

// leadKinds are the signals that best point at a cause, in order of preference. Health findings come last:
// they always exist for an unhealthy job, so there is always something to look at.
var leadKinds = []string{SigExceptionCause, SigException, SigPodTerminated, SigPodWaiting, SigPodUnschedulable, SigOperatorError, SigEvent, SigLog, SigHealth}

// quietKinds are left out of "also seen" for a healthy job: probes fail and errors are logged on healthy jobs too.
var quietKinds = []string{SigEvent, SigLog, SigHealth}

// maxUnexplained bounds the list, to keep why on one screen.
const maxUnexplained = 4

// Newest is when the newest evidence of the finding happened; zero when none of its signals knows.
func (f *Finding) Newest() time.Time {
	var t time.Time
	for _, s := range f.Evidence {
		if s.At.After(t) {
			t = s.At
		}
	}
	return t
}

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
	// A superseded finding's evidence is explained too: it is the symptom of the finding that superseded it.
	matched := slices.Clone(findings)
	findings = slices.DeleteFunc(findings, func(f Finding) bool { return superseded[f.ID] })
	slices.SortStableFunc(findings, func(a, b Finding) int {
		return cmp.Or(cmp.Compare(b.Severity, a.Severity),
			cmp.Compare(slices.Index(rankLayers, a.Layer), slices.Index(rankLayers, b.Layer)))
	})
	v := &Verdict{Health: ev.Health.Health, Findings: findings}
	if v.Findings == nil {
		v.Findings = []Finding{}
	}
	unhealthy := ev.Health.Health >= model.Degraded
	v.Explained = slices.ContainsFunc(findings, func(f Finding) bool { return f.Severity >= model.Degraded })
	if unhealthy || len(findings) > 0 {
		v.Unexplained = unexplained(signals, matched, unhealthy && !v.Explained)
	}
	return v, nil
}

// unexplained picks, by preference, the signals no finding used and that do not repeat a used one, such as the
// operator's event that carries an exception already in a finding.
func unexplained(signals []Signal, findings []Finding, withQuiet bool) []Signal {
	var used []string
	for _, f := range findings {
		for _, s := range f.Evidence {
			// Short texts, such as a status value, would make every signal that contains them look repeated.
			if n := norm(s.Text); len(n) >= minRepeat && !strings.HasPrefix(s.Kind, "status.") {
				used = append(used, n)
			}
		}
	}
	repeats := func(s Signal) bool {
		n := norm(s.Text)
		return slices.ContainsFunc(used, func(u string) bool { return strings.Contains(n, u) || strings.Contains(u, n) })
	}
	var out []Signal
	for _, kind := range leadKinds {
		if !withQuiet && slices.Contains(quietKinds, kind) {
			continue
		}
		if kind == SigHealth && len(out) > 0 {
			break // health findings are the last resort, not more of the same
		}
		for _, s := range signals {
			if s.Kind == kind && !repeats(s) {
				out = append(out, s)
				break
			}
		}
		if len(out) == maxUnexplained {
			break
		}
	}
	return out
}

// minRepeat is the shortest text that counts when one signal repeats another.
const minRepeat = 20

// norm reduces a signal's text to its first 80 characters without leading words that end in a colon, such as an
// event's reason and an exception's class, so that the same failure in an event ("JobException:
// org.apache.flink.runtime.JobException: Recovery is suppressed"), an exception and the operator error compares
// equal.
func norm(s string) string {
	s = strings.ToLower(s)
	for {
		before, rest, ok := strings.Cut(s, ": ")
		if !ok || strings.Contains(before, " ") {
			break
		}
		s = rest
	}
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}
