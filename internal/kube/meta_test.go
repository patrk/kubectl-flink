package kube

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestObjectMetaAccessor(t *testing.T) {
	for _, obj := range []any{&FlinkDeployment{}, &FlinkSessionJob{}, &Pod{}} {
		m, err := meta.Accessor(obj)
		if err != nil {
			t.Fatalf("%T: %v", obj, err)
		}
		ts := metav1.NewTime(time.Unix(1, 0))
		m.SetNamespace("ns")
		m.SetName("n")
		m.SetUID(types.UID("u"))
		m.SetResourceVersion("7")
		m.SetGeneration(3)
		m.SetCreationTimestamp(ts)
		m.SetDeletionTimestamp(&ts)
		m.SetLabels(map[string]string{"a": "b"})
		m.SetAnnotations(map[string]string{"c": "d"})
		if m.GetNamespace() != "ns" || m.GetName() != "n" || m.GetUID() != "u" || m.GetResourceVersion() != "7" ||
			m.GetGeneration() != 3 || !m.GetCreationTimestamp().Time.Equal(ts.Time) || !m.GetDeletionTimestamp().Equal(&ts) ||
			m.GetLabels()["a"] != "b" || m.GetAnnotations()["c"] != "d" {
			t.Errorf("%T: kept fields did not round-trip", obj)
		}
		// Fields the tool does not keep are accepted and dropped.
		grace := int64(30)
		m.SetGenerateName("g")
		m.SetSelfLink("s")
		m.SetDeletionGracePeriodSeconds(&grace)
		m.SetFinalizers([]string{"f"})
		m.SetOwnerReferences([]metav1.OwnerReference{{Name: "o"}})
		m.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "m"}})
		if m.GetGenerateName() != "" || m.GetSelfLink() != "" || m.GetDeletionGracePeriodSeconds() != nil ||
			m.GetFinalizers() != nil || m.GetOwnerReferences() != nil || m.GetManagedFields() != nil {
			t.Errorf("%T: dropped fields were kept", obj)
		}
	}
}
