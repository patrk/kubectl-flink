package kube

import (
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/randfill"
)

func filler() *randfill.Filler {
	return randfill.New().NilChance(0).NumElements(1, 3).Funcs(
		func(t *metav1.Time, c randfill.Continue) { *t = metav1.NewTime(time.Unix(c.Int63n(1<<32), 0).UTC()) },
		func(q *resource.Quantity, c randfill.Continue) {
			*q = *resource.NewQuantity(c.Int63n(1<<20), resource.BinarySI)
		},
	)
}

// TestDeepCopy fills every field, copies, and checks the copy is equal and shares no pointer, map or slice.
func TestDeepCopy(t *testing.T) {
	f := filler()
	for _, obj := range []runtime.Object{
		&FlinkDeployment{}, &FlinkSessionJob{}, &Pod{}, &FlinkDeploymentList{}, &FlinkSessionJobList{}, &PodList{},
		&Event{}, &EventList{}, &ConfigMap{}, &ConfigMapList{}, &FlinkStateSnapshot{}, &FlinkStateSnapshotList{},
	} {
		for range 20 {
			f.Fill(obj)
			cp := obj.DeepCopyObject()
			if !reflect.DeepEqual(obj, cp) {
				t.Fatalf("%T: copy differs\n%+v\n%+v", obj, obj, cp)
			}
			assertNoAlias(t, reflect.TypeOf(obj).String(), reflect.ValueOf(obj), reflect.ValueOf(cp))
		}
	}
}

var locationType = reflect.TypeFor[*time.Location]()

func assertNoAlias(t *testing.T, path string, a, b reflect.Value) {
	t.Helper()
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || a.Type() == locationType {
			return
		}
		if a.Pointer() == b.Pointer() {
			t.Fatalf("%s: pointer shared", path)
		}
		assertNoAlias(t, path, a.Elem(), b.Elem())
	case reflect.Map:
		if a.Len() == 0 {
			return
		}
		if a.Pointer() == b.Pointer() {
			t.Fatalf("%s: map shared", path)
		}
		for _, k := range a.MapKeys() {
			assertNoAlias(t, path+"["+k.String()+"]", a.MapIndex(k), b.MapIndex(k))
		}
	case reflect.Slice:
		if a.Len() == 0 {
			return
		}
		if a.Pointer() == b.Pointer() {
			t.Fatalf("%s: slice shared", path)
		}
		for i := range a.Len() {
			assertNoAlias(t, path, a.Index(i), b.Index(i))
		}
	case reflect.Struct:
		for i := range a.NumField() {
			assertNoAlias(t, path+"."+a.Type().Field(i).Name, a.Field(i), b.Field(i))
		}
	case reflect.Interface:
		if !a.IsNil() {
			assertNoAlias(t, path, a.Elem(), b.Elem())
		}
	}
}
