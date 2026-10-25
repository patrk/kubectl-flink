package model

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/patrk/kubectl-flink/internal/kube"
)

// ClusterPods summarises the JobManager and TaskManager pods of one Flink cluster.
type ClusterPods struct {
	JM, TM Component
	// CPU and Memory are the summed container requests of all pods.
	CPU, Memory resource.Quantity
}

// Component summarises the pods of one Flink component.
type Component struct {
	Total, Ready int
	Restarts     int32
	// NotReadySince is the earliest time a present pod became not ready; zero when all are ready or unknown.
	NotReadySince time.Time
	// Waiting is why a container cannot start, such as ImagePullBackOff, with the pod's name. Normal startup
	// states such as ContainerCreating are left out.
	Waiting string
	// LastTermination is the most recent container termination, such as "OOMKilled (exit 137)".
	LastTermination string
	lastTerminated  time.Time
}

// SummarisePods groups Flink pods by cluster, keyed by namespace/app.
func SummarisePods(pods []kube.Pod) map[string]*ClusterPods {
	out := map[string]*ClusterPods{}
	for i := range pods {
		p := &pods[i]
		// Evicted, completed and terminating pods are leftovers or on their way out: they neither serve the job
		// nor count against the desired replicas, and their requests are released or about to be.
		if p.Status.Phase == "Failed" || p.Status.Phase == "Succeeded" || p.DeletionTimestamp != nil {
			continue
		}
		key := kube.ClusterKey(p.Namespace, p.Labels[kube.LabelApp])
		cp := out[key]
		if cp == nil {
			cp = &ClusterPods{}
			out[key] = cp
		}
		var c *Component
		switch p.Labels[kube.LabelComponent] {
		case kube.ComponentJM:
			c = &cp.JM
		case kube.ComponentTM:
			c = &cp.TM
		default:
			continue
		}
		c.add(p)
		for _, ctr := range p.Spec.Containers {
			if q, ok := ctr.Resources.Requests["cpu"]; ok {
				cp.CPU.Add(q)
			}
			if q, ok := ctr.Resources.Requests["memory"]; ok {
				cp.Memory.Add(q)
			}
		}
	}
	return out
}

// startingReasons are waiting reasons of a container that is starting normally, not failing.
var startingReasons = map[string]bool{"": true, "ContainerCreating": true, "PodInitializing": true}

func (c *Component) add(p *kube.Pod) {
	c.Total++
	ready := p.Status.Phase == "Running"
	var readySince time.Time
	for _, cond := range p.Status.Conditions {
		if cond.Type == "Ready" {
			ready = ready && cond.Status == "True"
			readySince = cond.LastTransitionTime.Time
		}
	}
	if ready {
		c.Ready++
	} else if !readySince.IsZero() && (c.NotReadySince.IsZero() || readySince.Before(c.NotReadySince)) {
		c.NotReadySince = readySince
	}
	for _, cs := range p.Status.ContainerStatuses {
		c.Restarts += cs.RestartCount
		if w := cs.State.Waiting; w != nil && !startingReasons[w.Reason] && c.Waiting == "" {
			c.Waiting = w.Reason + " (" + p.Name + ")"
		}
		if t := cs.LastState.Terminated; t != nil && t.FinishedAt.After(c.lastTerminated) {
			c.lastTerminated = t.FinishedAt.Time
			c.LastTermination = fmt.Sprintf("%s (exit %d)", t.Reason, t.ExitCode)
		}
	}
}

// AttachPods sets Job.Pods of each FlinkDeployment from a SummarisePods result. read reports whether the pods
// of a namespace were read: a deployment there without pods gets an empty summary, one elsewhere keeps nil, so
// that its pod checks are skipped rather than reporting every TaskManager missing. Session jobs get none: their
// TaskManagers belong to the session cluster and are judged there.
func AttachPods(jobs []Job, pods map[string]*ClusterPods, read func(namespace string) bool) {
	for i := range jobs {
		j := &jobs[i]
		if j.Kind != KindDeployment || !read(j.Namespace) {
			continue
		}
		if cp := pods[kube.ClusterKey(j.Namespace, j.Name)]; cp != nil {
			j.Pods = cp
		} else {
			j.Pods = &ClusterPods{}
		}
	}
}

// AttachSnapshots sets each job's newest completed FlinkStateSnapshot, matched by the operator's job-reference
// labels.
func AttachSnapshots(jobs []Job, snaps []kube.FlinkStateSnapshot) {
	type key struct{ ns, kind, name string }
	newest := map[key]*kube.FlinkStateSnapshot{}
	for i := range snaps {
		s := &snaps[i]
		if s.Status.State != "COMPLETED" {
			continue
		}
		k := key{s.Namespace, s.Labels[kube.LabelSnapshotKind], s.Labels[kube.LabelSnapshotJob]}
		if cur := newest[k]; cur == nil || s.Result().After(cur.Result()) {
			newest[k] = s
		}
	}
	for i := range jobs {
		j := &jobs[i]
		if s := newest[key{j.Namespace, string(j.Kind), j.Name}]; s != nil {
			j.LastSnapshot = s.Result()
		}
	}
}
