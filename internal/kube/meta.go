package kube

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ObjectMeta is the part of metav1.ObjectMeta every cached object keeps. It implements metav1.Object, which
// informers need for keys and resource versions; setters for fields it does not keep do nothing.
type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               types.UID         `json:"uid,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	CreationTimestamp metav1.Time       `json:"creationTimestamp"`
	DeletionTimestamp *metav1.Time      `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	// Annotations are kept because a WatchList bookmark marks the end of the initial events with one.
	Annotations Annotations `json:"annotations,omitempty"`
}

var _ metav1.Object = &ObjectMeta{}

// GetNamespace implements metav1.Object.
func (m *ObjectMeta) GetNamespace() string { return m.Namespace }

// SetNamespace implements metav1.Object.
func (m *ObjectMeta) SetNamespace(ns string) { m.Namespace = ns }

// GetName implements metav1.Object.
func (m *ObjectMeta) GetName() string { return m.Name }

// SetName implements metav1.Object.
func (m *ObjectMeta) SetName(n string) { m.Name = n }

// GetGenerateName implements metav1.Object.
func (m *ObjectMeta) GetGenerateName() string { return "" }

// SetGenerateName implements metav1.Object.
func (m *ObjectMeta) SetGenerateName(string) {}

// GetUID implements metav1.Object.
func (m *ObjectMeta) GetUID() types.UID { return m.UID }

// SetUID implements metav1.Object.
func (m *ObjectMeta) SetUID(uid types.UID) { m.UID = uid }

// GetResourceVersion implements metav1.Object.
func (m *ObjectMeta) GetResourceVersion() string { return m.ResourceVersion }

// SetResourceVersion implements metav1.Object.
func (m *ObjectMeta) SetResourceVersion(v string) { m.ResourceVersion = v }

// GetGeneration implements metav1.Object.
func (m *ObjectMeta) GetGeneration() int64 { return m.Generation }

// SetGeneration implements metav1.Object.
func (m *ObjectMeta) SetGeneration(g int64) { m.Generation = g }

// GetSelfLink implements metav1.Object.
func (m *ObjectMeta) GetSelfLink() string { return "" }

// SetSelfLink implements metav1.Object.
func (m *ObjectMeta) SetSelfLink(string) {}

// GetCreationTimestamp implements metav1.Object.
func (m *ObjectMeta) GetCreationTimestamp() metav1.Time { return m.CreationTimestamp }

// SetCreationTimestamp implements metav1.Object.
func (m *ObjectMeta) SetCreationTimestamp(t metav1.Time) { m.CreationTimestamp = t }

// GetDeletionTimestamp implements metav1.Object.
func (m *ObjectMeta) GetDeletionTimestamp() *metav1.Time { return m.DeletionTimestamp }

// SetDeletionTimestamp implements metav1.Object.
func (m *ObjectMeta) SetDeletionTimestamp(t *metav1.Time) { m.DeletionTimestamp = t }

// GetDeletionGracePeriodSeconds implements metav1.Object.
func (m *ObjectMeta) GetDeletionGracePeriodSeconds() *int64 { return nil }

// SetDeletionGracePeriodSeconds implements metav1.Object.
func (m *ObjectMeta) SetDeletionGracePeriodSeconds(*int64) {}

// GetLabels implements metav1.Object.
func (m *ObjectMeta) GetLabels() map[string]string { return m.Labels }

// SetLabels implements metav1.Object.
func (m *ObjectMeta) SetLabels(l map[string]string) { m.Labels = l }

// GetAnnotations implements metav1.Object.
func (m *ObjectMeta) GetAnnotations() map[string]string { return m.Annotations }

// SetAnnotations implements metav1.Object.
func (m *ObjectMeta) SetAnnotations(a map[string]string) { m.Annotations = a }

// GetFinalizers implements metav1.Object.
func (m *ObjectMeta) GetFinalizers() []string { return nil }

// SetFinalizers implements metav1.Object.
func (m *ObjectMeta) SetFinalizers([]string) {}

// GetOwnerReferences implements metav1.Object.
func (m *ObjectMeta) GetOwnerReferences() []metav1.OwnerReference { return nil }

// SetOwnerReferences implements metav1.Object.
func (m *ObjectMeta) SetOwnerReferences([]metav1.OwnerReference) {}

// GetManagedFields implements metav1.Object; managed fields are summarised into Metadata.Managers instead.
func (m *ObjectMeta) GetManagedFields() []metav1.ManagedFieldsEntry { return nil }

// SetManagedFields implements metav1.Object.
func (m *ObjectMeta) SetManagedFields([]metav1.ManagedFieldsEntry) {}
