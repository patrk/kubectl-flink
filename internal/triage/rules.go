package triage

import (
	_ "embed"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"text/template"

	"sigs.k8s.io/yaml"

	"github.com/patrk/kubectl-flink/internal/model"
)

// RuleSpec is a rule as written in YAML, by this repository or by a profile.
type RuleSpec struct {
	ID       string `json:"id"`
	Layer    Layer  `json:"layer"`
	Severity string `json:"severity"` // a health value; OK marks a note about the past, such as a recovered restart
	// When lists conditions that must all hold; Any lists conditions of which one must hold; Optional ones add
	// captures and evidence when they hold and never stop a match.
	When       []Condition `json:"when,omitempty"`
	Any        []Condition `json:"any,omitempty"`
	Optional   []Condition `json:"optional,omitempty"`
	Supersedes []string    `json:"supersedes,omitempty"`
	// Title, Explain and Remedy are templates over the named captures of the matching conditions, plus job,
	// namespace and name.
	Title   string `json:"title"`
	Explain string `json:"explain"`
	Remedy  string `json:"remedy,omitempty"`
}

// Condition matches a signal kind's text against a regular expression.
type Condition struct {
	Signal string `json:"signal"`
	Match  string `json:"match"`
}

// Rule is a validated, compiled rule.
type Rule struct {
	RuleSpec
	severity model.Health
	when     []compiled
	any      []compiled
	optional []compiled
	title    *template.Template
	explain  *template.Template
	remedy   *template.Template
}

type compiled struct {
	kind string
	re   *regexp.Regexp
}

//go:embed rules.yaml
var defaultRules []byte

// DefaultRules returns the rules that ship with the tool.
func DefaultRules() []*Rule {
	rules, err := ParseRules(defaultRules)
	if err != nil {
		panic("embedded rules: " + err.Error())
	}
	return rules
}

// ParseRules reads and validates a YAML document of the form {rules: [...]}.
func ParseRules(b []byte) ([]*Rule, error) {
	var doc struct {
		Rules []RuleSpec `json:"rules"`
	}
	if err := yaml.UnmarshalStrict(b, &doc); err != nil {
		return nil, err
	}
	return CompileRules(doc.Rules)
}

// builtins are template keys every rule may use.
var builtins = []string{"job", "namespace", "name"}

// CompileRules validates specs: known signal kinds and layers, compiling expressions, unique ids, and
// templates that use only captures their conditions define.
func CompileRules(specs []RuleSpec) ([]*Rule, error) {
	var errs []error
	seen := map[string]bool{}
	var out []*Rule
	for i := range specs {
		r, err := compile(specs[i])
		if err != nil {
			errs = append(errs, fmt.Errorf("rule %q: %w", specs[i].ID, err))
			continue
		}
		if seen[r.ID] {
			errs = append(errs, fmt.Errorf("rule %q: id used twice", r.ID))
		}
		seen[r.ID] = true
		out = append(out, r)
	}
	return out, errors.Join(errs...)
}

func compile(s RuleSpec) (*Rule, error) {
	r := &Rule{RuleSpec: s}
	if s.ID == "" || s.Title == "" || s.Explain == "" {
		return nil, errors.New("id, title and explain are required")
	}
	if !slices.Contains(Layers, s.Layer) {
		return nil, fmt.Errorf("unknown layer %q", s.Layer)
	}
	var err error
	if r.severity, err = model.ParseHealth(s.Severity); err != nil {
		return nil, err
	}
	if len(s.When) == 0 && len(s.Any) == 0 {
		return nil, errors.New("needs a when or any condition")
	}
	captures := slices.Clone(builtins)
	conds := func(cs []Condition) ([]compiled, error) {
		out := make([]compiled, 0, len(cs))
		for _, c := range cs {
			if !slices.Contains(Kinds, c.Signal) {
				return nil, fmt.Errorf("unknown signal %q", c.Signal)
			}
			re, err := regexp.Compile(c.Match)
			if err != nil {
				return nil, err
			}
			for _, n := range re.SubexpNames() {
				if n != "" {
					captures = append(captures, n)
				}
			}
			out = append(out, compiled{kind: c.Signal, re: re})
		}
		return out, nil
	}
	if r.when, err = conds(s.When); err != nil {
		return nil, err
	}
	if r.any, err = conds(s.Any); err != nil {
		return nil, err
	}
	if r.optional, err = conds(s.Optional); err != nil {
		return nil, err
	}
	// Templates are checked by executing them with every capture set, so that a misspelt key fails at load.
	probe := map[string]string{}
	for _, c := range captures {
		probe[c] = "x"
	}
	for _, t := range []struct {
		dst  **template.Template
		text string
	}{{&r.title, s.Title}, {&r.explain, s.Explain}, {&r.remedy, s.Remedy}} {
		tmpl, err := template.New(s.ID).Option("missingkey=error").Parse(t.text)
		if err != nil {
			return nil, err
		}
		if err := tmpl.Execute(&strings.Builder{}, probe); err != nil {
			return nil, err
		}
		*t.dst = tmpl
	}
	return r, nil
}

// Finding is a rule that matched, with the signals that made it match.
type Finding struct {
	ID       string       `json:"id"`
	Layer    Layer        `json:"layer"`
	Severity model.Health `json:"severity"`
	Title    string       `json:"title"`
	Explain  string       `json:"explain"`
	Remedy   string       `json:"remedy,omitempty"`
	Evidence []Signal     `json:"evidence"`
	// Past marks a finding whose evidence all happened before the job's current run: the job has run without it
	// since, so it is told as history, at OK, and not as the cause of the present.
	Past bool `json:"past,omitempty"`
	rule *Rule
}

// match tries a condition against the signals and returns the matching signals and the captures of the first.
func (c *compiled) match(signals []Signal) (hits []Signal, captures map[string]string) {
	for _, s := range signals {
		if s.Kind != c.kind {
			continue
		}
		m := c.re.FindStringSubmatch(s.Text)
		if m == nil {
			continue
		}
		hits = append(hits, s)
		if captures == nil {
			captures = map[string]string{}
			for i, n := range c.re.SubexpNames() {
				if n != "" {
					captures[n] = m[i]
				}
			}
		}
	}
	return hits, captures
}

// Evaluate runs rules over signals and returns the findings in rule order.
func Evaluate(rules []*Rule, signals []Signal, job *model.Job) ([]Finding, error) {
	var out []Finding
	for _, r := range rules {
		data := map[string]string{"job": job.Key(), "namespace": job.Namespace, "name": job.Name}
		var evidence []Signal
		ok := true
		for i := range r.when {
			hits, caps := r.when[i].match(signals)
			if hits == nil {
				ok = false
				break
			}
			evidence = append(evidence, hits...)
			for k, v := range caps {
				data[k] = v
			}
		}
		if !ok {
			continue
		}
		if len(r.any) > 0 {
			matched := false
			for i := range r.any {
				if hits, caps := r.any[i].match(signals); hits != nil {
					matched = true
					evidence = append(evidence, hits...)
					for k, v := range caps {
						if _, set := data[k]; !set {
							data[k] = v
						}
					}
				}
			}
			if !matched {
				continue
			}
		}
		for i := range r.optional {
			if hits, caps := r.optional[i].match(signals); hits != nil {
				evidence = append(evidence, hits...)
				for k, v := range caps {
					if _, set := data[k]; !set {
						data[k] = v
					}
				}
			}
		}
		// A capture of a condition that did not match is empty, not missing.
		for _, n := range r.captureNames() {
			if _, set := data[n]; !set {
				data[n] = ""
			}
		}
		f := Finding{ID: r.ID, Layer: r.Layer, Severity: r.severity, Evidence: dedupe(evidence), rule: r}
		var err error
		if f.Title, err = execute(r.title, data); err == nil {
			if f.Explain, err = execute(r.explain, data); err == nil {
				f.Remedy, err = execute(r.remedy, data)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", r.ID, err)
		}
		out = append(out, f)
	}
	return out, nil
}

func (r *Rule) captureNames() []string {
	var out []string
	for _, cs := range [][]compiled{r.when, r.any, r.optional} {
		for _, c := range cs {
			for _, n := range c.re.SubexpNames() {
				if n != "" {
					out = append(out, n)
				}
			}
		}
	}
	return out
}

func execute(t *template.Template, data map[string]string) (string, error) {
	var b strings.Builder
	err := t.Execute(&b, data)
	return strings.TrimSpace(b.String()), err
}

func dedupe(signals []Signal) []Signal {
	slices.SortStableFunc(signals, func(a, b Signal) int {
		if c := strings.Compare(a.Kind, b.Kind); c != 0 {
			return c
		}
		return strings.Compare(a.Text, b.Text)
	})
	return slices.Compact(signals)
}
