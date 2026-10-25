package model

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/patrk/kubectl-flink/internal/kube"
)

// Owner is the tool whose desired state a job's spec comes from, inferred from the resource's own labels,
// annotations and field managers. Nothing else is read, so it costs no request and needs no further permission;
// From says what it was inferred from, because it is an inference.
type Owner struct {
	// Tool is "Argo CD", "Flux", "Helm", or the tool a profile rule names.
	Tool string `json:"tool"`
	// Name is the Argo CD Application, Flux Kustomization or HelmRelease, or Helm release, as namespace/name
	// when the namespace is known.
	Name string `json:"name,omitempty"`
	// From is the evidence, such as "annotation argocd.argoproj.io/tracking-id".
	From string `json:"from"`
	// Reverts says whether the tool undoes a manual edit of the spec.
	Reverts Reverts `json:"reverts"`
	// Edits are the spec fields that managers other than the owner own: changes made outside the owner, which
	// it has not taken back. The operator is not among them.
	Edits []SpecWriter `json:"edits,omitempty"`
}

// Reverts is whether an owner undoes a manual edit.
type Reverts string

// Revert behaviours.
const (
	// RevertsAlways: the tool re-applies its state on every reconcile (Flux).
	RevertsAlways Reverts = "always"
	// RevertsSelfHeal: Argo CD re-applies only when the Application has self-heal on, which is not on the resource.
	RevertsSelfHeal Reverts = "self-heal"
	// RevertsNextRelease: the edit lasts until the next helm upgrade.
	RevertsNextRelease Reverts = "next-release"
	// RevertsNo: nothing is known to re-apply the spec.
	RevertsNo Reverts = "no"
	// RevertsUnknown: a profile rule names the owner but not what it does with an edit.
	RevertsUnknown Reverts = "unknown"
)

// Tools.
const (
	ToolArgoCD = "Argo CD"
	ToolFlux   = "Flux"
	ToolHelm   = "Helm"
)

// Markers Argo CD, Flux and Helm put on the resources they apply.
const (
	argoTrackingID       = "argocd.argoproj.io/tracking-id"
	instanceLabel        = "app.kubernetes.io/instance"
	managedByLabel       = "app.kubernetes.io/managed-by"
	fluxKustomizeName    = "kustomize.toolkit.fluxcd.io/name"
	fluxKustomizeNS      = "kustomize.toolkit.fluxcd.io/namespace"
	fluxReconcile        = "kustomize.toolkit.fluxcd.io/reconcile"
	fluxHelmName         = "helm.toolkit.fluxcd.io/name"
	fluxHelmNS           = "helm.toolkit.fluxcd.io/namespace"
	helmReleaseName      = "meta.helm.sh/release-name"
	helmReleaseNamespace = "meta.helm.sh/release-namespace"
)

// ownManagers are the field managers each tool writes with. Argo CD writes as argocd-controller, and as
// argocd-application-controller in older versions; see ownManager for kubectl-client-side-apply.
var ownManagers = map[string][]string{
	ToolArgoCD: {"argocd-controller", "argocd-application-controller"},
	ToolFlux:   {"kustomize-controller", "helm-controller"},
	ToolHelm:   {"helm"},
}

// OwnerRule names an owner the built-in inference does not know, such as a platform's own deployment service. A
// rule matches when every condition it sets holds; rules are tried in order, before the built-in inference.
type OwnerRule struct {
	// Tool is how the owner is called, such as "Deployment service".
	Tool string `json:"tool"`
	// Manager is the field manager the owner writes the spec with.
	Manager string `json:"manager,omitempty"`
	// Label and Annotation are a key, or key=value, the owner puts on the resource.
	Label      string `json:"label,omitempty"`
	Annotation string `json:"annotation,omitempty"`
	// NameLabel and NameAnnotation are keys whose value names the owner's own record, such as a deployment ID.
	NameLabel      string `json:"nameLabel,omitempty"`
	NameAnnotation string `json:"nameAnnotation,omitempty"`
	// Reverts says whether the owner undoes a manual edit; empty is unknown.
	Reverts Reverts `json:"reverts,omitempty"`
}

// Validate reports a rule that cannot match or says something unknown.
func (r *OwnerRule) Validate() error {
	switch {
	case r.Tool == "":
		return errors.New("an owner rule needs a tool")
	case r.Manager == "" && r.Label == "" && r.Annotation == "":
		return fmt.Errorf("owner rule %q needs a manager, label or annotation to match", r.Tool)
	}
	switch r.Reverts {
	case "", RevertsAlways, RevertsNextRelease, RevertsNo:
		return nil
	}
	return fmt.Errorf("owner rule %q: reverts is %q; use %s, %s or %s", r.Tool, r.Reverts, RevertsAlways, RevertsNextRelease, RevertsNo)
}

func (r *OwnerRule) match(j *Job) (Owner, bool) {
	var from []string
	if r.Manager != "" {
		if !slices.ContainsFunc(j.SpecWriters, func(w SpecWriter) bool { return w.Manager == r.Manager }) {
			return Owner{}, false
		}
		from = append(from, "field manager "+r.Manager)
	}
	for _, c := range []struct {
		kind, cond string
		set        map[string]string
	}{{"label", r.Label, j.Labels}, {"annotation", r.Annotation, j.Annotations}} {
		if c.cond == "" {
			continue
		}
		key, want, hasValue := strings.Cut(c.cond, "=")
		v, ok := c.set[key]
		if !ok || hasValue && v != want {
			return Owner{}, false
		}
		from = append(from, c.kind+" "+c.cond)
	}
	o := Owner{Tool: r.Tool, Name: cmp.Or(j.Labels[r.NameLabel], j.Annotations[r.NameAnnotation]), From: "profile: " + strings.Join(from, ", "),
		Reverts: cmp.Or(r.Reverts, RevertsUnknown)}
	if r.Manager != "" {
		for _, w := range j.SpecWriters {
			if w.Manager != r.Manager {
				o.Edits = append(o.Edits, w)
			}
		}
	}
	return o, true
}

// InferOwner infers the owner of j from its labels, annotations and spec writers, trying rules first. It is zero
// when no tool is known; the spec writers then speak for themselves.
func InferOwner(j *Job, rules []OwnerRule) Owner {
	for i := range rules {
		if o, ok := rules[i].match(j); ok {
			return o
		}
	}
	o := markedOwner(j)
	if o.Tool == "" {
		return o
	}
	for _, w := range j.SpecWriters {
		if !ownManager(o.Tool, w.Manager, j.SpecWriters) {
			o.Edits = append(o.Edits, w)
		}
	}
	return o
}

func markedOwner(j *Job) Owner {
	labels, annotations, writers := j.Labels, j.Annotations, j.SpecWriters
	if id, ok := annotations[argoTrackingID]; ok {
		// <app>:<group>/<kind>:<namespace>/<name>; an annotation copied from another resource does not name this
		// one, and Argo CD ignores it too.
		// The namespace is empty when the manifest had none and Argo CD applied it to the destination namespace.
		app, res, _ := strings.Cut(id, ":")
		gk := kube.Group + "/" + string(j.Kind) + ":"
		if res == gk+j.Namespace+"/"+j.Name || res == gk+"/"+j.Name {
			return Owner{Tool: ToolArgoCD, Name: argoApp(app), From: "annotation " + argoTrackingID, Reverts: RevertsSelfHeal}
		}
	}
	if name := labels[fluxKustomizeName]; name != "" {
		o := Owner{Tool: ToolFlux, Name: qualified(labels[fluxKustomizeNS], name), From: "labels kustomize.toolkit.fluxcd.io", Reverts: RevertsAlways}
		if annotations[fluxReconcile] == "disabled" || labels[fluxReconcile] == "disabled" {
			o.Reverts, o.From = RevertsNo, o.From+", reconcile disabled"
		}
		return o
	}
	if name := labels[fluxHelmName]; name != "" {
		return Owner{Tool: ToolFlux, Name: qualified(labels[fluxHelmNS], name), From: "labels helm.toolkit.fluxcd.io", Reverts: RevertsAlways}
	}
	// Argo CD's label tracking uses the label Helm sets too, so the label counts only when Argo CD wrote the spec.
	// It comes before Helm's markers: a chart Argo CD renders keeps the chart's managed-by: Helm label.
	if app := labels[instanceLabel]; app != "" && wroteBy(writers, ToolArgoCD) {
		return Owner{Tool: ToolArgoCD, Name: argoApp(app), From: "label " + instanceLabel + " and field manager", Reverts: RevertsSelfHeal}
	}
	if name := annotations[helmReleaseName]; name != "" {
		return Owner{Tool: ToolHelm, Name: qualified(annotations[helmReleaseNamespace], name), From: "annotation " + helmReleaseName, Reverts: RevertsNextRelease}
	}
	if labels[managedByLabel] == ToolHelm {
		return Owner{Tool: ToolHelm, Name: labels[instanceLabel], From: "label " + managedByLabel, Reverts: RevertsNextRelease}
	}
	// No marker: a GitOps controller's field manager still says who applies the spec.
	for _, tool := range []string{ToolArgoCD, ToolFlux} {
		if wroteBy(writers, tool) {
			o := Owner{Tool: tool, From: "field manager", Reverts: RevertsAlways}
			if tool == ToolArgoCD {
				o.Reverts = RevertsSelfHeal
			}
			return o
		}
	}
	return Owner{}
}

// csaManager is the field manager of kubectl apply, which older Argo CD versions' client-side apply also wrote as.
const csaManager = "kubectl-client-side-apply"

// ownManager reports whether manager is the tool itself. kubectl-client-side-apply counts as Argo CD only when
// Argo CD's own manager has not written the spec: then it is an older Argo CD; otherwise it is a person.
func ownManager(tool, manager string, writers []SpecWriter) bool {
	if tool == ToolArgoCD && manager == csaManager {
		return !wroteBy(writers, ToolArgoCD)
	}
	return slices.Contains(ownManagers[tool], manager)
}

// wroteBy reports whether one of tool's own managers wrote the spec.
func wroteBy(writers []SpecWriter, tool string) bool {
	return slices.ContainsFunc(writers, func(w SpecWriter) bool { return slices.Contains(ownManagers[tool], w.Manager) })
}

// argoApp turns Argo CD's instance name into namespace/name: an Application outside Argo CD's own namespace is
// written <namespace>_<name>, one inside it by name alone, and that namespace is not on the resource.
func argoApp(instance string) string {
	if ns, name, ok := strings.Cut(instance, "_"); ok {
		return ns + "/" + name
	}
	return instance
}

func qualified(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

// Token is the owner as one word for a table cell, such as "argo-cd:payments-jobs".
func (o Owner) Token() string {
	t := strings.ToLower(strings.ReplaceAll(o.Tool, " ", "-"))
	if o.Name == "" {
		return t
	}
	return t + ":" + o.Name
}

// String is the owner in a few words, such as "Argo CD payments-jobs".
func (o Owner) String() string {
	if o.Name == "" {
		return o.Tool
	}
	return o.Tool + " " + o.Name
}
