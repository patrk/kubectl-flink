// Package profile loads the optional user profile: where to look and how to judge. Everything works without one.
package profile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/redact"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// APIVersion is the profile file format this build reads.
const APIVersion = "kubectl-flink/v1alpha1"

// EnvPath overrides the profile location.
const EnvPath = "KUBECTL_FLINK_CONFIG"

// File is the profile file: several profiles, chosen by the current context.
type File struct {
	APIVersion string    `json:"apiVersion"`
	Profiles   []Profile `json:"profiles"`
}

// Profile is the configuration for a set of contexts.
type Profile struct {
	Name string `json:"name"`
	// Match lists context name patterns, where * matches any run of characters, "/" and ":" included, so that
	// EKS ARNs and GKE names match too; the first profile with a matching pattern is used.
	Match []string `json:"match"`
	// Scopes are read together; a scope without a context means the current one.
	Scopes []kube.Scope `json:"scopes,omitempty"`
	// IDLabels are the label keys, on the resource or its pod template, whose values identify a job, such as a
	// business or use-case ID. Job arguments and ls filters match their values exactly. Empty means every label.
	IDLabels []string `json:"idLabels,omitempty"`
	// Rules are added to the default verdict rules; a rule with the id of a default rule replaces it.
	Rules []triage.RuleSpec `json:"rules,omitempty"`
	// DisableRules lists default rule ids not to use.
	DisableRules []string `json:"disableRules,omitempty"`
	// Report adds redaction patterns for the report bundle and history, for secrets only this platform has.
	Report ReportSettings `json:"report,omitzero"`
	// Owners name the tools that deploy jobs beyond Argo CD, Flux and Helm, such as a platform's deployment
	// service; they are tried in order, before the built-in inference.
	Owners []model.OwnerRule `json:"owners,omitempty"`
	// ReconcileInterval is the operator's reconcile interval, from which health thresholds are derived.
	ReconcileInterval metav1.Duration `json:"reconcileInterval,omitzero"`
}

// ReportSettings extend the redaction of the report bundle.
type ReportSettings struct {
	// RedactKeys are regular expressions for further keys whose values are secret.
	RedactKeys []string `json:"redactKeys,omitempty"`
	// RedactText are regular expressions masked in free text; with two groups, the groups are kept around the
	// masked part.
	RedactText []string `json:"redactText,omitempty"`
}

// Redactor returns the redactor for the profile: the defaults plus the profile's patterns.
func (p *Profile) Redactor() (*redact.Redactor, error) {
	return redact.NewRedactor(p.Report.RedactKeys, p.Report.RedactText)
}

// Default is used when no profile matches: the current context and namespace, and the operator's defaults.
func Default() Profile {
	return Profile{Name: "default", ReconcileInterval: metav1.Duration{Duration: time.Minute}}
}

// Path is where the profile is read from: $KUBECTL_FLINK_CONFIG, else the XDG config directory.
func Path() (string, error) {
	if p := os.Getenv(EnvPath); p != "" {
		return p, nil
	}
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		// ~/.config on macOS too, where kubectl users look, not ~/Library/Application Support.
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "kubectl-flink", "config.yaml"), nil
}

// Load reads and validates the profile file. A missing file is not an error and yields an empty File.
func Load(p string) (*File, error) {
	b, err := os.ReadFile(p) // #nosec G304 -- the user's own profile path
	if errors.Is(err, fs.ErrNotExist) {
		return &File{APIVersion: APIVersion}, nil
	}
	if err != nil {
		return nil, err
	}
	var f File
	if err := yaml.UnmarshalStrict(b, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	if err := f.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	return &f, nil
}

// Validate checks the file beyond its syntax.
func (f *File) Validate() error {
	if f.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion %q is not supported; want %q", f.APIVersion, APIVersion)
	}
	var errs []error
	for i := range f.Profiles {
		p := &f.Profiles[i]
		where := fmt.Sprintf("profiles[%d] %q", i, p.Name)
		if len(p.Match) == 0 {
			errs = append(errs, fmt.Errorf("%s: match is empty", where))
		}
		seen := map[string]bool{}
		for _, s := range p.Scopes {
			if seen[s.Context] {
				errs = append(errs, fmt.Errorf("%s: context %q appears in two scopes", where, s.Context))
			}
			seen[s.Context] = true
		}
		for j := range p.Owners {
			if err := p.Owners[j].Validate(); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", where, err))
			}
		}
		own, err := triage.CompileRules(p.Rules)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", where, err))
		}
		// A misspelt rule id would silently do nothing, so every id must name a default or own rule.
		known := map[string]bool{}
		for _, r := range append(triage.DefaultRules(), own...) {
			known[r.ID] = true
		}
		for _, id := range p.DisableRules {
			if !known[id] {
				errs = append(errs, fmt.Errorf("%s: disableRules: no rule %q", where, id))
			}
		}
		for j := range p.Rules {
			for _, id := range p.Rules[j].Supersedes {
				if !known[id] {
					errs = append(errs, fmt.Errorf("%s: rule %q supersedes unknown rule %q", where, p.Rules[j].ID, id))
				}
			}
		}
		if _, err := p.Redactor(); err != nil {
			errs = append(errs, fmt.Errorf("%s: report: %w", where, err))
		}
		if p.ReconcileInterval.Duration < 0 {
			errs = append(errs, fmt.Errorf("%s: reconcileInterval is negative", where))
		}
	}
	return errors.Join(errs...)
}

// For returns the first profile matching context, or Default.
func (f *File) For(context string) Profile {
	for i := range f.Profiles {
		p := f.Profiles[i]
		for _, m := range p.Match {
			if glob(m, context) {
				if p.ReconcileInterval.Duration == 0 {
					p.ReconcileInterval = Default().ReconcileInterval
				}
				return p
			}
		}
	}
	return Default()
}

// Thresholds are the health thresholds for the profile's reconcile interval.
func (p *Profile) Thresholds() model.Thresholds {
	return model.ThresholdsFor(p.ReconcileInterval.Duration)
}

// Flags are the kubectl flags that narrow or widen the scopes.
type Flags struct {
	// Context is --context; when set, only that context is read.
	Context string
	// Namespace is -n; when set, it replaces the namespaces of every scope.
	Namespace string
	// AllNamespaces is -A.
	AllNamespaces bool
}

// Resolve resolves the profile's scopes against the flags and the current context. An explicit --context reads
// only that context, keeping the namespaces the profile gives it.
func (p *Profile) Resolve(current string, fl Flags) []kube.Scope {
	var out []kube.Scope
	for _, s := range p.Scopes {
		if s.Context == "" {
			s.Context = current
		}
		if fl.Context != "" && s.Context != fl.Context {
			continue
		}
		// A scope without a context and one naming the current context are the same; merge them so that the
		// context is read once. A scope without namespaces means the context's namespace, which survives the
		// merge as "".
		if i := slices.IndexFunc(out, func(o kube.Scope) bool { return o.Context == s.Context }); i >= 0 {
			o := &out[i]
			o.Namespaces = mergeNamespaces(o.Namespaces, s.Namespaces)
			o.AllNamespaces = o.AllNamespaces || s.AllNamespaces
			continue
		}
		s.Namespaces = slices.Clone(s.Namespaces)
		out = append(out, s)
	}
	if len(out) == 0 {
		out = []kube.Scope{{Context: current}}
	}
	for i := range out {
		if fl.Namespace != "" {
			out[i].Namespaces = []string{fl.Namespace}
		}
		if fl.AllNamespaces {
			out[i].AllNamespaces = true
		}
	}
	return out
}

// glob matches s against pattern, where * matches any run of characters and every other character itself.
func glob(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return s == pattern
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	for _, p := range parts[1 : len(parts)-1] {
		i := strings.Index(s, p)
		if i < 0 {
			return false
		}
		s = s[i+len(p):]
	}
	return strings.HasSuffix(s, parts[len(parts)-1])
}

func mergeNamespaces(a, b []string) []string {
	if len(a) == 0 {
		a = []string{""}
	}
	if len(b) == 0 {
		b = []string{""}
	}
	out := slices.Clone(a)
	for _, ns := range b {
		if !slices.Contains(out, ns) {
			out = append(out, ns)
		}
	}
	return out
}

// VerdictRules returns the verdict rules for the profile: defaults minus disabled ones, with the profile's own rules
// replacing defaults of the same id and added after the rest.
func (p *Profile) VerdictRules(defaults []*triage.Rule) ([]*triage.Rule, error) {
	own, err := triage.CompileRules(p.Rules)
	if err != nil {
		return nil, fmt.Errorf("profile %s: %w", p.Name, err)
	}
	out := make([]*triage.Rule, 0, len(defaults)+len(own))
	for _, r := range defaults {
		replaced := slices.ContainsFunc(own, func(o *triage.Rule) bool { return o.ID == r.ID })
		if !replaced && !slices.Contains(p.DisableRules, r.ID) {
			out = append(out, r)
		}
	}
	return append(out, own...), nil
}
