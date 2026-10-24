package kube

import (
	"encoding/json"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ConfigMap keeps only a ConfigMap's keys. HA ConfigMaps hold job plans and checkpoint pointers as values of
// several kilobytes each; what tells whether they are left over is which keys exist.
type ConfigMap struct {
	metav1.TypeMeta `json:",inline"`
	ObjectMeta      `json:"metadata"`
	Keys            KeySet `json:"data,omitempty"`
}

// KeySet decodes a map's keys, sorted, without its values.
type KeySet []string

// UnmarshalJSON implements json.Unmarshaler.
func (k *KeySet) UnmarshalJSON(b []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	keys := make(KeySet, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	*k = keys
	return nil
}

// MarshalJSON implements json.Marshaler, as a map with empty values, so that the result decodes again.
func (k KeySet) MarshalJSON() ([]byte, error) {
	m := make(map[string]string, len(k))
	for _, key := range k {
		m[key] = ""
	}
	return json.Marshal(m)
}

// ConfigMapList is a list of ConfigMaps.
type ConfigMapList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ConfigMap `json:"items"`
}

func (l *ConfigMapList) appendPage(p runtime.Object) {
	l.Items = append(l.Items, p.(*ConfigMapList).Items...)
}

// DeepCopyObject implements runtime.Object.
func (c *ConfigMap) DeepCopyObject() runtime.Object {
	out := *c
	c.DeepCopyInto(&out.ObjectMeta)
	out.Keys = slices.Clone(c.Keys)
	return &out
}

// DeepCopyObject implements runtime.Object.
func (l *ConfigMapList) DeepCopyObject() runtime.Object {
	out := &ConfigMapList{TypeMeta: l.TypeMeta}
	l.DeepCopyInto(&out.ListMeta)
	out.Items = copyItems(l.Items, func(in, o *ConfigMap) { *o = *in.DeepCopyObject().(*ConfigMap) })
	return out
}

// ConfigMaps are the core/v1 ConfigMaps, by key only.
var ConfigMaps = Resource{
	GVR:     CoreGroupVersion.WithResource("configmaps"),
	newObj:  func() runtime.Object { return &ConfigMap{} },
	newList: func() runtime.Object { return &ConfigMapList{} },
	core:    true,
}
