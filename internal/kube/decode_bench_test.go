package kube

import (
	"encoding/json"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// BenchmarkDecode compares the typed minimal structs with unstructured decoding, which is what a dynamic
// informer would do, on a real FlinkDeployment.
func BenchmarkDecode(b *testing.B) {
	data, err := os.ReadFile("../../testdata/fixtures/operator-1.16.1-flink-2.2/flinkdeployment.json")
	if err != nil {
		b.Fatal(err)
	}
	b.Run("typed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var fd FlinkDeployment
			if err := json.Unmarshal(data, &fd); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unstructured", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var u unstructured.Unstructured
			if err := u.UnmarshalJSON(data); err != nil {
				b.Fatal(err)
			}
		}
	})
}
