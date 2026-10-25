package kube

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// GroupVersions the tool reads.
var (
	FlinkGroupVersion = schema.GroupVersion{Group: Group, Version: Version}
	CoreGroupVersion  = schema.GroupVersion{Version: "v1"}
)

// FlinkDeploymentList is a list of FlinkDeployments.
type FlinkDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []FlinkDeployment `json:"items"`
}

// FlinkSessionJobList is a list of FlinkSessionJobs.
type FlinkSessionJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []FlinkSessionJob `json:"items"`
}

// PodList is a list of Pods.
type PodList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []Pod `json:"items"`
}

// Scheme is a private scheme holding only the minimal types, so that the REST client decodes straight into them.
var Scheme = newScheme()

// Codecs decode the minimal types; there is nothing to convert between versions.
var Codecs = serializer.NewCodecFactory(Scheme).WithoutConversion()

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypes(FlinkGroupVersion, &FlinkDeployment{}, &FlinkDeploymentList{}, &FlinkSessionJob{}, &FlinkSessionJobList{},
		&FlinkStateSnapshot{}, &FlinkStateSnapshotList{})
	s.AddKnownTypes(CoreGroupVersion, &Pod{}, &PodList{}, &Event{}, &EventList{}, &ConfigMap{}, &ConfigMapList{})
	// WatchEvent, Status and the list options of each group version.
	metav1.AddToGroupVersion(s, FlinkGroupVersion)
	metav1.AddToGroupVersion(s, CoreGroupVersion)
	return s
}
