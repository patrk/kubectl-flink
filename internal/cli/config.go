package cli

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/profile"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

func newConfigCommand(o *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Show and check the profile",
		Long: `The profile, at $KUBECTL_FLINK_CONFIG or ~/.config/kubectl-flink/config.yaml, says where to look (contexts
and namespaces), which labels identify jobs, extra verdict rules and redaction patterns. Without one, the current
context and namespace are used.`,
	}
	cmd.AddCommand(newConfigViewCommand(o), newConfigValidateCommand(o))
	return cmd
}

// EffectiveConfig is what config view shows: the profile in force for the current context and what it resolves to.
type EffectiveConfig struct {
	Path       string                 `json:"path"`
	FileExists bool                   `json:"fileExists"`
	Context    string                 `json:"context"`
	Profile    string                 `json:"profile"`
	Scopes     []ScopeView            `json:"scopes"`
	IDLabels   []string               `json:"idLabels,omitempty"`
	Thresholds ThresholdsView         `json:"thresholds"`
	Rules      RulesView              `json:"rules"`
	Report     profile.ReportSettings `json:"report,omitzero"`
}

// ScopeView is a scope with the reason it cannot be read, if it cannot.
type ScopeView struct {
	kube.Scope `json:",inline"`
	Error      string `json:"error,omitempty"`
}

// ThresholdsView shows the health thresholds as durations.
type ThresholdsView struct {
	ReconcileInterval string `json:"reconcileInterval"`
	Grace             string `json:"grace"`
	DegradedAfter     string `json:"degradedAfter"`
	StuckAfter        string `json:"stuckAfter"`
	DriftAfter        string `json:"driftAfter"`
}

// RulesView counts the verdict rules in force.
type RulesView struct {
	Default  int      `json:"default"`
	Disabled []string `json:"disabled,omitempty"`
	Own      []string `json:"own,omitempty"`
	Total    int      `json:"total"`
}

func newConfigViewCommand(o *Options) *cobra.Command {
	var output string
	var all bool
	cmd := &cobra.Command{
		Use:   "view",
		Short: "Show the profile in force for the current context and what it resolves to",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if output != "yaml" && output != "json" {
				return &exitError{code: ExitUsage, err: fmt.Errorf("unknown output %q, use yaml or json", output)}
			}
			v, err := o.effectiveConfig(all)
			if err != nil {
				return err
			}
			if output == "json" {
				return render.JSON(o.Out, v)
			}
			return render.YAML(o.Out, v)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "yaml", "Output format: yaml, json")
	cmd.Flags().BoolVarP(&all, "all-namespaces", "A", false, "Resolve the scopes as ls -A would")
	return cmd
}

func (o *Options) effectiveConfig(all bool) (*EffectiveConfig, error) {
	path, err := profile.Path()
	if err != nil {
		return nil, err
	}
	srcs, p, err := o.sources(all)
	if err != nil {
		return nil, err
	}
	cs, err := kube.NewContexts(o.ConfigFlags)
	if err != nil {
		return nil, err
	}
	_, statErr := os.Stat(path)
	v := &EffectiveConfig{Path: path, FileExists: statErr == nil, Context: cs.Current(), Profile: p.Name,
		IDLabels: p.IDLabels, Report: p.Report}
	for _, s := range srcs {
		sv := ScopeView{Scope: s.Scope}
		if s.Err != nil {
			sv.Error = s.Err.Error()
		}
		v.Scopes = append(v.Scopes, sv)
	}
	th := p.Thresholds()
	v.Thresholds = ThresholdsView{ReconcileInterval: p.ReconcileInterval.Duration.String(), Grace: th.Grace.String(),
		DegradedAfter: th.DegradedAfter.String(), StuckAfter: th.StuckAfter.String(), DriftAfter: th.DriftAfter.String()}
	rules, err := p.VerdictRules(triage.DefaultRules())
	if err != nil {
		return nil, err
	}
	v.Rules = RulesView{Default: len(triage.DefaultRules()), Disabled: p.DisableRules, Total: len(rules)}
	for i := range p.Rules {
		v.Rules.Own = append(v.Rules.Own, p.Rules[i].ID)
	}
	return v, nil
}

func newConfigValidateCommand(o *Options) *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Check a profile file: syntax, patterns, rules, and that its contexts exist in the kubeconfig",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			path := file
			if path == "" {
				var err error
				if path, err = profile.Path(); err != nil {
					return err
				}
			}
			if _, err := os.Stat(path); err != nil {
				return fmt.Errorf("no profile at %s: %w", path, err)
			}
			f, err := profile.Load(path)
			if err != nil {
				return err
			}
			// Contexts a scope names but the kubeconfig does not have would be an error row in ls; say so now.
			var warnings []string
			if cs, err := kube.NewContexts(o.ConfigFlags); err != nil {
				warnings = append(warnings, fmt.Sprintf("contexts not checked: %v", err))
			} else {
				known := cs.Names()
				for i := range f.Profiles {
					for _, s := range f.Profiles[i].Scopes {
						if s.Context != "" && !slices.Contains(known, s.Context) {
							warnings = append(warnings, fmt.Sprintf("profile %q: context %q is not in the kubeconfig", f.Profiles[i].Name, s.Context))
						}
					}
				}
			}
			for _, w := range warnings {
				_, _ = fmt.Fprintln(o.ErrOut, "warning: "+w)
			}
			names := make([]string, len(f.Profiles))
			for i := range f.Profiles {
				names[i] = f.Profiles[i].Name
			}
			_, err = fmt.Fprintf(o.Out, "%s is valid: %d profiles (%s)\n", path, len(f.Profiles), strings.Join(names, ", "))
			return err
		},
	}
	cmd.Flags().StringVarP(&file, "filename", "f", "", "Profile file to check instead of the one in use")
	_ = cmd.MarkFlagFilename("filename", "yaml", "yml")
	return cmd
}
