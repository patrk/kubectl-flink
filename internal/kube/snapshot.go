package kube

import (
	"encoding/json"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Snapshot labels the operator sets once it has processed a FlinkStateSnapshot. An operator that does not process
// snapshots leaves them without labels and status.
const (
	LabelSnapshotKind  = "job-reference.kind"
	LabelSnapshotJob   = "job-reference.name"
	LabelSnapshotState = "snapshot.state"
)

// FlinkStateSnapshot is the subset of a flink.apache.org/v1beta1 FlinkStateSnapshot the tool reads.
type FlinkStateSnapshot struct {
	metav1.TypeMeta `json:",inline"`
	ObjectMeta      `json:"metadata"`
	Spec            SnapshotSpec   `json:"spec"`
	Status          SnapshotStatus `json:"status"`
}

// SnapshotSpec says which job and whether a savepoint or a checkpoint.
type SnapshotSpec struct {
	JobReference struct {
		Kind string `json:"kind,omitempty"`
		Name string `json:"name,omitempty"`
	} `json:"jobReference"`
	Savepoint *struct {
		Path       string `json:"path,omitempty"`
		FormatType string `json:"formatType,omitempty"`
	} `json:"savepoint,omitempty"`
	// Checkpoint is set ({}) for a checkpoint; kept raw, as it has no fields the tool reads.
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
}

// SnapshotStatus is the operator's report on a snapshot. Timestamps are RFC 3339 strings.
type SnapshotStatus struct {
	State            string `json:"state,omitempty"` // TRIGGER_PENDING, IN_PROGRESS, COMPLETED, FAILED, ABANDONED
	Path             string `json:"path,omitempty"`
	TriggerID        string `json:"triggerId,omitempty"`
	TriggerTimestamp string `json:"triggerTimestamp,omitempty"`
	ResultTimestamp  string `json:"resultTimestamp,omitempty"`
	Error            string `json:"error,omitempty"`
	Failures         int    `json:"failures,omitempty"`
}

// Type is SAVEPOINT or CHECKPOINT.
func (s *FlinkStateSnapshot) Type() string {
	if len(s.Spec.Checkpoint) > 0 && string(s.Spec.Checkpoint) != "null" {
		return "CHECKPOINT"
	}
	return "SAVEPOINT"
}

// Trigger is how the snapshot came about (MANUAL, PERIODIC, UPGRADE), as the operator labels it.
func (s *FlinkStateSnapshot) Trigger() string { return s.Labels["snapshot.trigger-type"] }

// Result is when the snapshot finished or last failed; zero when unknown.
func (s *FlinkStateSnapshot) Result() time.Time { return parseTime(s.Status.ResultTimestamp) }

// Triggered is when the snapshot was triggered; zero when unknown.
func (s *FlinkStateSnapshot) Triggered() time.Time { return parseTime(s.Status.TriggerTimestamp) }

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// FlinkStateSnapshotList is a list of FlinkStateSnapshots.
type FlinkStateSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []FlinkStateSnapshot `json:"items"`
}

func (l *FlinkStateSnapshotList) appendPage(p runtime.Object) {
	l.Items = append(l.Items, p.(*FlinkStateSnapshotList).Items...)
}

// DeepCopyObject implements runtime.Object.
func (s *FlinkStateSnapshot) DeepCopyObject() runtime.Object {
	out := *s
	s.DeepCopyInto(&out.ObjectMeta)
	if s.Spec.Savepoint != nil {
		sp := *s.Spec.Savepoint
		out.Spec.Savepoint = &sp
	}
	out.Spec.Checkpoint = slices.Clone(s.Spec.Checkpoint)
	return &out
}

// DeepCopyObject implements runtime.Object.
func (l *FlinkStateSnapshotList) DeepCopyObject() runtime.Object {
	out := &FlinkStateSnapshotList{TypeMeta: l.TypeMeta}
	l.DeepCopyInto(&out.ListMeta)
	out.Items = copyItems(l.Items, func(in, o *FlinkStateSnapshot) { *o = *in.DeepCopyObject().(*FlinkStateSnapshot) })
	return out
}

// Snapshots are the FlinkStateSnapshots.
var Snapshots = Resource{
	GVR:     FlinkGroupVersion.WithResource("flinkstatesnapshots"),
	newObj:  func() runtime.Object { return &FlinkStateSnapshot{} },
	newList: func() runtime.Object { return &FlinkStateSnapshotList{} },
}
