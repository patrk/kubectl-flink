package model

import (
	"cmp"
	"reflect"
	"testing"
	"time"
)

func TestInferOwner(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	argo := SpecWriter{Manager: "argocd-controller", Fields: []string{"flinkVersion", "job"}, Time: t0}
	edit := SpecWriter{Manager: "kubectl-edit", Fields: []string{"job"}, Time: t0.Add(time.Hour)}
	patch := SpecWriter{Manager: "kubectl-patch", Fields: []string{"restartNonce"}, Time: t0.Add(2 * time.Hour)}
	meta := func(labels, annotations map[string]string) *Job {
		return &Job{Namespace: "payments", Name: "fraud-scoring", Labels: labels, Annotations: annotations}
	}
	tests := []struct {
		name    string
		kind    Kind
		meta    *Job
		writers []SpecWriter
		want    Owner
	}{
		{
			name:    "argo annotation, a manual edit kept",
			meta:    meta(nil, map[string]string{argoTrackingID: "payments-jobs:flink.apache.org/FlinkDeployment:payments/fraud-scoring"}),
			writers: []SpecWriter{argo, edit},
			want: Owner{Tool: ToolArgoCD, Name: "payments-jobs", From: "annotation argocd.argoproj.io/tracking-id",
				Reverts: RevertsSelfHeal, Edits: []SpecWriter{edit}},
		},
		{
			name: "argo application outside its control-plane namespace",
			meta: meta(nil, map[string]string{argoTrackingID: "teams_payments-jobs:flink.apache.org/FlinkDeployment:payments/fraud-scoring"}),
			want: Owner{Tool: ToolArgoCD, Name: "teams/payments-jobs", From: "annotation argocd.argoproj.io/tracking-id", Reverts: RevertsSelfHeal},
		},
		{
			name:    "argo annotation copied from another resource is ignored",
			meta:    meta(nil, map[string]string{argoTrackingID: "other:flink.apache.org/FlinkDeployment:payments/settlement"}),
			writers: []SpecWriter{edit},
		},
		{
			name: "argo annotation without the resource's namespace",
			meta: meta(nil, map[string]string{argoTrackingID: "jobs:flink.apache.org/FlinkDeployment:/fraud-scoring"}),
			want: Owner{Tool: ToolArgoCD, Name: "jobs", From: "annotation argocd.argoproj.io/tracking-id", Reverts: RevertsSelfHeal},
		},
		{
			name:    "kubectl apply next to argo's own manager is a person",
			meta:    meta(nil, map[string]string{argoTrackingID: "jobs:flink.apache.org/FlinkDeployment:payments/fraud-scoring"}),
			writers: []SpecWriter{argo, {Manager: csaManager, Fields: []string{"job"}, Time: t0}},
			want: Owner{Tool: ToolArgoCD, Name: "jobs", From: "annotation argocd.argoproj.io/tracking-id", Reverts: RevertsSelfHeal,
				Edits: []SpecWriter{{Manager: csaManager, Fields: []string{"job"}, Time: t0}}},
		},
		{
			name: "argo annotation of a session job",
			kind: KindSessionJob,
			meta: meta(nil, map[string]string{argoTrackingID: "jobs:flink.apache.org/FlinkSessionJob:payments/fraud-scoring"}),
			want: Owner{Tool: ToolArgoCD, Name: "jobs", From: "annotation argocd.argoproj.io/tracking-id", Reverts: RevertsSelfHeal},
		},
		{
			name:    "argo label tracking needs argo's field manager",
			meta:    meta(map[string]string{instanceLabel: "payments-jobs", managedByLabel: "Helm"}, nil),
			writers: []SpecWriter{argo},
			want:    Owner{Tool: ToolArgoCD, Name: "payments-jobs", From: "label app.kubernetes.io/instance and field manager", Reverts: RevertsSelfHeal},
		},
		{
			name:    "the instance label alone is helm's",
			meta:    meta(map[string]string{instanceLabel: "fraud", managedByLabel: "Helm"}, map[string]string{helmReleaseName: "fraud", helmReleaseNamespace: "payments"}),
			writers: []SpecWriter{{Manager: "helm", Fields: []string{"job"}, Time: t0}, patch},
			want: Owner{Tool: ToolHelm, Name: "payments/fraud", From: "annotation meta.helm.sh/release-name",
				Reverts: RevertsNextRelease, Edits: []SpecWriter{patch}},
		},
		{
			name:    "argo's old client-side apply is not an edit",
			meta:    meta(nil, map[string]string{argoTrackingID: "jobs:flink.apache.org/FlinkDeployment:payments/fraud-scoring"}),
			writers: []SpecWriter{{Manager: "kubectl-client-side-apply", Fields: []string{"job"}, Time: t0}},
			want:    Owner{Tool: ToolArgoCD, Name: "jobs", From: "annotation argocd.argoproj.io/tracking-id", Reverts: RevertsSelfHeal},
		},
		{
			name: "flux kustomization",
			meta: meta(map[string]string{fluxKustomizeName: "apps", fluxKustomizeNS: "flux-system"}, nil),
			want: Owner{Tool: ToolFlux, Name: "flux-system/apps", From: "labels kustomize.toolkit.fluxcd.io", Reverts: RevertsAlways},
		},
		{
			name: "flux reconcile disabled",
			meta: meta(map[string]string{fluxKustomizeName: "apps", fluxKustomizeNS: "flux-system"}, map[string]string{fluxReconcile: "disabled"}),
			want: Owner{Tool: ToolFlux, Name: "flux-system/apps", From: "labels kustomize.toolkit.fluxcd.io, reconcile disabled", Reverts: RevertsNo},
		},
		{
			name: "flux helm release",
			meta: meta(map[string]string{fluxHelmName: "fraud", fluxHelmNS: "payments"}, map[string]string{helmReleaseName: "payments-fraud"}),
			want: Owner{Tool: ToolFlux, Name: "payments/fraud", From: "labels helm.toolkit.fluxcd.io", Reverts: RevertsAlways},
		},
		{
			name:    "argo field manager without markers",
			meta:    meta(nil, nil),
			writers: []SpecWriter{argo, patch},
			want:    Owner{Tool: ToolArgoCD, From: "field manager", Reverts: RevertsSelfHeal, Edits: []SpecWriter{patch}},
		},
		{name: "no tool", meta: meta(nil, nil), writers: []SpecWriter{patch, edit}},
		{name: "nothing wrote the spec", meta: meta(nil, nil)},
	}
	for _, tt := range tests {
		j := tt.meta
		j.Kind, j.SpecWriters = cmp.Or(tt.kind, KindDeployment), tt.writers
		if got := InferOwner(j, nil); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", tt.name, got, tt.want)
		}
	}
}

func TestOwnerRules(t *testing.T) {
	svc := SpecWriter{Manager: "deploy-svc", Fields: []string{"job"}}
	edit := SpecWriter{Manager: "kubectl-edit", Fields: []string{"job"}}
	j := &Job{Kind: KindDeployment, Namespace: "payments", Name: "fraud-scoring",
		Labels:      map[string]string{"platform.example.com/deployment-id": "4711", instanceLabel: "fraud"},
		Annotations: map[string]string{helmReleaseName: "fraud"},
		SpecWriters: []SpecWriter{svc, edit}}
	rules := []OwnerRule{
		{Tool: "never", Label: "platform.example.com/deployment-id=1"},
		{Tool: "Deployment service", Manager: "deploy-svc", Label: "platform.example.com/deployment-id", NameLabel: "platform.example.com/deployment-id", Reverts: RevertsAlways},
	}
	want := Owner{Tool: "Deployment service", Name: "4711", From: "profile: field manager deploy-svc, label platform.example.com/deployment-id",
		Reverts: RevertsAlways, Edits: []SpecWriter{edit}}
	if got := InferOwner(j, rules); !reflect.DeepEqual(got, want) {
		t.Errorf("rule:\n got %+v\nwant %+v", got, want)
	}
	// A rule that does not match leaves the built-in inference.
	if got := InferOwner(j, rules[:1]); got.Tool != ToolHelm {
		t.Errorf("no match: %+v", got)
	}
	if got := InferOwner(j, []OwnerRule{{Tool: "svc", Annotation: helmReleaseName}}); got.Reverts != RevertsUnknown || got.Edits != nil {
		t.Errorf("annotation rule: %+v", got)
	}
	for _, bad := range []OwnerRule{{Manager: "x"}, {Tool: "x"}, {Tool: "x", Manager: "y", Reverts: "sometimes"}} {
		if bad.Validate() == nil {
			t.Errorf("%+v validated", bad)
		}
	}
}
