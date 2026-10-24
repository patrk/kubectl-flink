package kube

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// openAPI is the part of an OpenAPI v3 schema the test walks.
type openAPI struct {
	Properties           map[string]*openAPI `json:"properties"`
	Items                *openAPI            `json:"items"`
	AdditionalProperties json.RawMessage     `json:"additionalProperties"`
	PreserveUnknown      bool                `json:"x-kubernetes-preserve-unknown-fields"`
}

func loadCRDSchema(t *testing.T, path string) *openAPI {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	var crd struct {
		Spec struct {
			Versions []struct {
				Name   string `json:"name"`
				Schema struct {
					OpenAPIV3Schema *openAPI `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(b, &crd); err != nil {
		t.Fatal(err)
	}
	for _, v := range crd.Spec.Versions {
		if v.Name == Version {
			return v.Schema.OpenAPIV3Schema
		}
	}
	t.Fatalf("%s: no version %s", path, Version)
	return nil
}

var (
	unmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	timeType        = reflect.TypeFor[metav1.Time]()
)

// leaf reports whether the test stops at t: scalars and types that decode themselves.
func leaf(t reflect.Type) bool {
	if t == timeType || reflect.PointerTo(t).Implements(unmarshalerType) {
		return true
	}
	switch t.Kind() {
	case reflect.Struct, reflect.Slice, reflect.Map:
		return false
	case reflect.Pointer:
		return leaf(t.Elem())
	default:
		return true
	}
}

// missing lists the JSON paths of t that s does not declare.
func missing(t reflect.Type, s *openAPI, path string) []string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if leaf(t) || s.PreserveUnknown && len(s.Properties) == 0 {
		return nil
	}
	switch t.Kind() {
	case reflect.Slice:
		if s.Items == nil {
			return []string{path + "[]"}
		}
		return missing(t.Elem(), s.Items, path+"[]")
	case reflect.Map:
		var ap openAPI
		if len(s.AdditionalProperties) == 0 || json.Unmarshal(s.AdditionalProperties, &ap) != nil {
			return []string{path + ".*"}
		}
		return missing(t.Elem(), &ap, path+".*")
	}
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if f.Anonymous && name == "" {
			out = append(out, missing(f.Type, s, path)...)
			continue
		}
		child, ok := s.Properties[name]
		if !ok {
			out = append(out, path+"."+name)
			continue
		}
		out = append(out, missing(f.Type, child, path+"."+name)...)
	}
	return out
}

// TestFieldsExistInCRDs checks that every spec and status field the tool reads is declared by each supported
// operator version's CRD, so that an upstream rename fails here instead of silently decoding as empty.
// Refresh the CRDs with hack/fetch-crds.sh.
func TestFieldsExistInCRDs(t *testing.T) {
	dirs, _ := filepath.Glob("../../testdata/crds/operator-*")
	if len(dirs) < 2 {
		t.Fatalf("found %d CRD versions, want at least 2", len(dirs))
	}
	for _, dir := range dirs {
		for file, typ := range map[string]reflect.Type{
			"flinkdeployments.yml.gz": reflect.TypeFor[FlinkDeployment](),
			"flinksessionjobs.yml.gz": reflect.TypeFor[FlinkSessionJob](),
		} {
			s := loadCRDSchema(t, filepath.Join(dir, file))
			for _, part := range []string{"Spec", "Status"} {
				f, _ := typ.FieldByName(part)
				name := strings.ToLower(part)
				if gaps := missing(f.Type, s.Properties[name], name); len(gaps) > 0 {
					t.Errorf("%s %s: not in the CRD: %s", filepath.Base(dir), typ.Name(), strings.Join(gaps, ", "))
				}
			}
		}
	}
}
