// Package model holds the normalised view of a Flink job that every command and the TUI render from.
package model

import (
	"time"

	"github.com/patrk/kubectl-flink/internal/kube"
)

// Kind is the resource a Job was built from.
type Kind string

// Resource kinds.
const (
	KindDeployment Kind = "FlinkDeployment"
	KindSessionJob Kind = "FlinkSessionJob"
)

// operatorManager is the field manager of the Flink operator's Kubernetes client. Its writes to the main
// resource (finalizers) do not make it an owner of the spec.
const operatorManager = "fabric8-kubernetes-client"

// Job is one FlinkDeployment or FlinkSessionJob, normalised.
type Job struct {
	Kind       Kind
	Context    string
	Namespace  string
	Name       string
	UID        string
	Generation int64
	Created    time.Time
	// DeletingSince is the deletion timestamp; zero when the resource is not being deleted.
	DeletingSince time.Time

	Labels    map[string]string
	PodLabels map[string]string

	// Spec.
	SessionCluster bool // a FlinkDeployment without spec.job
	DeploymentName string
	Image          string
	FlinkVersion   string
	SpecJobState   string
	UpgradeMode    string
	Parallelism    int32
	RestartNonce   *int64
	Config         kube.FlinkConfig

	// Status.
	Lifecycle          string
	JobState           string
	JobID              string
	JMStatus           string
	ReconcileState     string
	ObservedGeneration int64
	// TMReplicas is status.taskManager.replicas: the desired TaskManager count, not the running one.
	TMReplicas    int32
	Error         *StatusError
	JobStarted    time.Time
	JobStateSince time.Time
	ReconciledAt  time.Time
	FlinkRuntime  string // clusterInfo flink-version

	// SpecChangedAt is the latest write by any manager that owns spec fields, from managedFields. A manager's
	// time moves on any write it makes, so a later label change by a spec owner also moves it; drift and suspend
	// timers can therefore start late, never early.
	SpecChangedAt time.Time
	// SpecOwners are the managers that own spec fields, the operator excluded.
	SpecOwners []string

	// Pods is nil when pods could not be read; the pod checks are then skipped.
	Pods *ClusterPods
}

// Key is namespace/name, unique within a context.
func (j *Job) Key() string { return j.Namespace + "/" + j.Name }

// StateSince is when the job's current state began: the job state's update time, else the last reconcile.
func (j *Job) StateSince() time.Time {
	if !j.JobStateSince.IsZero() {
		return j.JobStateSince
	}
	return j.ReconciledAt
}

// Drift reports whether the operator has not yet acted on the latest spec.
func (j *Job) Drift() bool { return j.ObservedGeneration < j.Generation }

// FromDeployment normalises a FlinkDeployment.
func FromDeployment(context string, fd *kube.FlinkDeployment) Job {
	j := fromMeta(KindDeployment, context, &fd.Metadata)
	s := &fd.Spec
	j.Image, j.FlinkVersion, j.RestartNonce, j.Config = s.Image, s.FlinkVersion, s.RestartNonce, s.FlinkConfiguration
	j.PodLabels = s.PodTemplate.Metadata.Labels
	j.SessionCluster = s.Job == nil
	if s.Job != nil {
		j.SpecJobState, j.UpgradeMode, j.Parallelism = s.Job.State, s.Job.UpgradeMode, s.Job.Parallelism
	}
	fillCommon(&j, &fd.Status.CommonStatus)
	j.JMStatus = fd.Status.JobManagerDeploymentStatus
	if tm := fd.Status.TaskManager; tm != nil {
		j.TMReplicas = tm.Replicas
	}
	j.FlinkRuntime = fd.Status.ClusterInfo["flink-version"]
	return j
}

// FromSessionJob normalises a FlinkSessionJob.
func FromSessionJob(context string, sj *kube.FlinkSessionJob) Job {
	j := fromMeta(KindSessionJob, context, &sj.Metadata)
	j.DeploymentName, j.RestartNonce, j.Config = sj.Spec.DeploymentName, sj.Spec.RestartNonce, sj.Spec.FlinkConfiguration
	if sj.Spec.Job != nil {
		j.SpecJobState, j.UpgradeMode, j.Parallelism = sj.Spec.Job.State, sj.Spec.Job.UpgradeMode, sj.Spec.Job.Parallelism
	}
	fillCommon(&j, &sj.Status)
	return j
}

func fromMeta(kind Kind, context string, m *kube.Metadata) Job {
	j := Job{
		Kind:       kind,
		Context:    context,
		Namespace:  m.Namespace,
		Name:       m.Name,
		UID:        string(m.UID),
		Generation: m.Generation,
		Created:    m.CreationTimestamp.Time,
		Labels:     m.Labels,
	}
	if m.DeletionTimestamp != nil {
		j.DeletingSince = m.DeletionTimestamp.Time
	}
	for _, mgr := range m.Managers {
		if mgr.Subresource != "" || len(mgr.SpecFields) == 0 {
			continue
		}
		if mgr.Time.After(j.SpecChangedAt) {
			j.SpecChangedAt = mgr.Time.Time
		}
		if mgr.Name != operatorManager {
			j.SpecOwners = append(j.SpecOwners, mgr.Name)
		}
	}
	return j
}

func fillCommon(j *Job, s *kube.CommonStatus) {
	j.Lifecycle = s.LifecycleState
	j.JobState = s.JobStatus.State
	j.JobID = s.JobStatus.JobID
	j.ReconcileState = s.ReconciliationStatus.State
	j.ObservedGeneration = s.ObservedGeneration
	j.Error = ParseStatusError(s.Error)
	j.JobStarted = millis(s.JobStatus.StartTime)
	j.JobStateSince = millis(s.JobStatus.UpdateTime)
	j.ReconciledAt = millis(s.ReconciliationStatus.ReconciliationTimestamp)
}

func millis(e kube.EpochMilli) time.Time {
	if e == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(e))
}
