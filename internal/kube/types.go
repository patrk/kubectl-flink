// Package kube holds the minimal typed view of the Flink operator's resources and the clients that read them.
//
// The structs declare only the fields the tool reads. The JSON decoder skips everything else, such as
// lastReconciledSpec and pod templates, without allocating it, which keeps lists and caches small.
package kube

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Group and version of the operator's resources.
const (
	Group   = "flink.apache.org"
	Version = "v1beta1"
)

// FlinkDeployment is the subset of flink.apache.org/v1beta1 FlinkDeployment the tool reads.
type FlinkDeployment struct {
	metav1.TypeMeta `json:",inline"`
	Metadata        `json:"metadata"`
	Spec            FlinkDeploymentSpec   `json:"spec"`
	Status          FlinkDeploymentStatus `json:"status"`
}

// FlinkSessionJob is the subset of flink.apache.org/v1beta1 FlinkSessionJob the tool reads.
type FlinkSessionJob struct {
	metav1.TypeMeta `json:",inline"`
	Metadata        `json:"metadata"`
	Spec            FlinkSessionJobSpec `json:"spec"`
	Status          CommonStatus        `json:"status"`
}

// Metadata is ObjectMeta reduced to what health, selection and owner detection need. Managed fields are
// summarised while decoding, and the last-applied-configuration annotation is dropped.
type Metadata struct {
	ObjectMeta `json:",inline"`
	Managers   Managers `json:"managedFields,omitempty"`
}

// Annotations drops kubectl's last-applied-configuration, a full copy of the spec.
type Annotations map[string]string

const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// UnmarshalJSON implements json.Unmarshaler.
func (a *Annotations) UnmarshalJSON(b []byte) error {
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	delete(m, lastAppliedAnnotation)
	*a = m
	return nil
}

// Manager summarises one managedFields entry: who wrote the object, when, and which spec fields it owns.
type Manager struct {
	Name        string      `json:"name"`
	Operation   string      `json:"operation"`
	Subresource string      `json:"subresource,omitempty"`
	Time        metav1.Time `json:"time"`
	// SpecFields are the top-level spec fields the manager owns, without the "f:" prefix.
	SpecFields []string `json:"specFields,omitempty"`
}

// Managers decodes metadata.managedFields into summaries, keeping only the top level of fieldsV1.spec.
type Managers []Manager

// UnmarshalJSON implements json.Unmarshaler.
func (ms *Managers) UnmarshalJSON(b []byte) error {
	var raw []struct {
		Manager     string      `json:"manager"`
		Operation   string      `json:"operation"`
		Subresource string      `json:"subresource"`
		Time        metav1.Time `json:"time"`
		FieldsV1    struct {
			Spec map[string]json.RawMessage `json:"f:spec"`
		} `json:"fieldsV1"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	out := make(Managers, 0, len(raw))
	for _, r := range raw {
		m := Manager{Name: r.Manager, Operation: r.Operation, Subresource: r.Subresource, Time: r.Time}
		for k := range r.FieldsV1.Spec {
			if f, ok := strings.CutPrefix(k, "f:"); ok {
				m.SpecFields = append(m.SpecFields, f)
			}
		}
		sort.Strings(m.SpecFields)
		out = append(out, m)
	}
	*ms = out
	return nil
}

// FlinkDeploymentSpec is the subset of the FlinkDeployment spec the tool reads.
type FlinkDeploymentSpec struct {
	Image              string          `json:"image,omitempty"`
	FlinkVersion       string          `json:"flinkVersion,omitempty"`
	Mode               string          `json:"mode,omitempty"`
	ServiceAccount     string          `json:"serviceAccount,omitempty"`
	RestartNonce       *int64          `json:"restartNonce,omitempty"`
	FlinkConfiguration FlinkConfig     `json:"flinkConfiguration,omitempty"`
	Job                *JobSpec        `json:"job,omitempty"`
	JobManager         ComponentSpec   `json:"jobManager"`
	TaskManager        ComponentSpec   `json:"taskManager"`
	PodTemplate        PodTemplateMeta `json:"podTemplate"`
}

// FlinkSessionJobSpec is the subset of the FlinkSessionJob spec the tool reads.
type FlinkSessionJobSpec struct {
	DeploymentName     string      `json:"deploymentName"`
	RestartNonce       *int64      `json:"restartNonce,omitempty"`
	FlinkConfiguration FlinkConfig `json:"flinkConfiguration,omitempty"`
	Job                *JobSpec    `json:"job,omitempty"`
}

// JobSpec is the subset of spec.job the tool reads.
type JobSpec struct {
	JarURI      string `json:"jarURI,omitempty"`
	Parallelism int32  `json:"parallelism,omitempty"`
	State       string `json:"state,omitempty"`
	UpgradeMode string `json:"upgradeMode,omitempty"`
}

// ComponentSpec is the subset of spec.jobManager and spec.taskManager the tool reads.
type ComponentSpec struct {
	Replicas *int32           `json:"replicas,omitempty"`
	Resource ResourceSpec     `json:"resource"`
	Pod      *PodTemplateMeta `json:"podTemplate,omitempty"`
}

// ResourceSpec is the operator's resource shorthand.
type ResourceSpec struct {
	CPU    float64 `json:"cpu,omitempty"`
	Memory string  `json:"memory,omitempty"`
}

// PodTemplateMeta keeps only the labels of a pod template; platforms often put business IDs there.
type PodTemplateMeta struct {
	Metadata struct {
		Labels map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
}

// CommonStatus holds the status fields shared by FlinkDeployment and FlinkSessionJob.
type CommonStatus struct {
	LifecycleState       string               `json:"lifecycleState,omitempty"`
	Error                string               `json:"error,omitempty"`
	ObservedGeneration   int64                `json:"observedGeneration,omitempty"`
	JobStatus            JobStatus            `json:"jobStatus"`
	ReconciliationStatus ReconciliationStatus `json:"reconciliationStatus"`
}

// FlinkDeploymentStatus adds the cluster-level fields of a FlinkDeployment.
type FlinkDeploymentStatus struct {
	CommonStatus               `json:",inline"`
	JobManagerDeploymentStatus string             `json:"jobManagerDeploymentStatus,omitempty"`
	TaskManager                *TaskManagerStatus `json:"taskManager,omitempty"`
	ClusterInfo                map[string]string  `json:"clusterInfo,omitempty"`
	Conditions                 []metav1.Condition `json:"conditions,omitempty"`
}

// JobStatus is the subset of status.jobStatus the tool reads. Times are epoch milliseconds as strings.
type JobStatus struct {
	JobName    string     `json:"jobName,omitempty"`
	JobID      string     `json:"jobId,omitempty"`
	State      string     `json:"state,omitempty"`
	StartTime  EpochMilli `json:"startTime,omitempty"`
	UpdateTime EpochMilli `json:"updateTime,omitempty"`
}

// ReconciliationStatus skips lastReconciledSpec and lastStableSpec, which are full copies of the spec.
type ReconciliationStatus struct {
	State                   string     `json:"state,omitempty"`
	ReconciliationTimestamp EpochMilli `json:"reconciliationTimestamp,omitempty"`
}

// TaskManagerStatus is status.taskManager.
type TaskManagerStatus struct {
	LabelSelector string `json:"labelSelector,omitempty"`
	Replicas      int32  `json:"replicas,omitempty"`
}

// EpochMilli is a timestamp in epoch milliseconds, which the operator writes as a number or a string.
type EpochMilli int64

// UnmarshalJSON implements json.Unmarshaler.
func (e *EpochMilli) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*e = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("epoch milliseconds %q: %w", s, err)
	}
	*e = EpochMilli(v)
	return nil
}

// FlinkConfig is spec.flinkConfiguration flattened to dotted keys. The CRD accepts both flat keys and
// nested YAML, so {"high-availability": {"type": "kubernetes"}} becomes "high-availability.type".
type FlinkConfig map[string]string

// UnmarshalJSON implements json.Unmarshaler.
func (c *FlinkConfig) UnmarshalJSON(b []byte) error {
	// UseNumber keeps numbers as written; float64 would turn 1073741824 into 1.073741824e+09.
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return err
	}
	out := FlinkConfig{}
	flatten("", v, out)
	*c = out
	return nil
}

func flatten(prefix string, v any, out FlinkConfig) {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			flatten(key, child, out)
		}
	case nil:
		if prefix != "" {
			out[prefix] = ""
		}
	case string:
		out[prefix] = t
	case json.Number:
		out[prefix] = t.String()
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, fmt.Sprint(e))
		}
		out[prefix] = strings.Join(parts, ";")
	default:
		out[prefix] = fmt.Sprint(t)
	}
}
