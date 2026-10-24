package kube

import (
	"encoding/json"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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

// BenchmarkDecodeList decodes a 1,000-item FlinkDeploymentList through the scheme's codec, the path the
// REST client and informers take.
func BenchmarkDecodeList(b *testing.B) {
	data, err := os.ReadFile("../../testdata/fixtures/operator-1.16.1-flink-2.2/flinkdeployment.json")
	if err != nil {
		b.Fatal(err)
	}
	items := make([]json.RawMessage, 1000)
	for i := range items {
		items[i] = data
	}
	list, err := json.Marshal(map[string]any{
		"kind": "FlinkDeploymentList", "apiVersion": "flink.apache.org/v1beta1", "metadata": map[string]any{}, "items": items,
	})
	if err != nil {
		b.Fatal(err)
	}
	info, ok := runtime.SerializerInfoForMediaType(Codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
	if !ok {
		b.Fatal("no JSON serializer")
	}
	dec := Codecs.DecoderToVersion(info.Serializer, FlinkGroupVersion)
	b.Run("codec", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			obj, _, err := dec.Decode(list, nil, &FlinkDeploymentList{})
			if err != nil {
				b.Fatal(err)
			}
			if n := len(obj.(*FlinkDeploymentList).Items); n != 1000 {
				b.Fatalf("%d items", n)
			}
		}
	})
	b.Run("encoding-json", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var l FlinkDeploymentList
			if err := json.Unmarshal(list, &l); err != nil {
				b.Fatal(err)
			}
		}
	})
}
