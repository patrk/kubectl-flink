package kube

import (
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
)

// Informers hand out shared objects and copy them on request; these copies are written by hand because the
// types are small. TestDeepCopy fills every field at random and checks that no copy shares memory.

func ptrCopy[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// DeepCopyInto copies m into out.
func (m *ObjectMeta) DeepCopyInto(out *ObjectMeta) {
	*out = *m
	out.DeletionTimestamp = m.DeletionTimestamp.DeepCopy()
	out.Labels = maps.Clone(m.Labels)
	out.Annotations = maps.Clone(m.Annotations)
}

// DeepCopyInto copies m into out.
func (m *Metadata) DeepCopyInto(out *Metadata) {
	*out = *m
	m.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if m.Managers != nil {
		out.Managers = make(Managers, len(m.Managers))
		for i, mgr := range m.Managers {
			mgr.SpecFields = slices.Clone(mgr.SpecFields)
			out.Managers[i] = mgr
		}
	}
}

func (p *PodTemplateMeta) deepCopyInto(out *PodTemplateMeta) {
	*out = *p
	out.Metadata.Labels = maps.Clone(p.Metadata.Labels)
}

func (c *ComponentSpec) deepCopyInto(out *ComponentSpec) {
	*out = *c
	out.Replicas = ptrCopy(c.Replicas)
	if c.Pod != nil {
		out.Pod = new(PodTemplateMeta)
		c.Pod.deepCopyInto(out.Pod)
	}
}

// DeepCopy copies fd.
func (fd *FlinkDeployment) DeepCopy() *FlinkDeployment {
	if fd == nil {
		return nil
	}
	out := new(FlinkDeployment)
	fd.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies fd into out.
func (fd *FlinkDeployment) DeepCopyInto(out *FlinkDeployment) {
	*out = *fd
	fd.Metadata.DeepCopyInto(&out.Metadata)
	s, o := &fd.Spec, &out.Spec
	o.RestartNonce = ptrCopy(s.RestartNonce)
	o.FlinkConfiguration = maps.Clone(s.FlinkConfiguration)
	o.Job = ptrCopy(s.Job)
	s.JobManager.deepCopyInto(&o.JobManager)
	s.TaskManager.deepCopyInto(&o.TaskManager)
	s.PodTemplate.deepCopyInto(&o.PodTemplate)
	out.Status.TaskManager = ptrCopy(fd.Status.TaskManager)
	out.Status.ClusterInfo = maps.Clone(fd.Status.ClusterInfo)
	out.Status.Conditions = slices.Clone(fd.Status.Conditions)
}

// DeepCopyObject implements runtime.Object.
func (fd *FlinkDeployment) DeepCopyObject() runtime.Object { return fd.DeepCopy() }

// DeepCopy copies sj.
func (sj *FlinkSessionJob) DeepCopy() *FlinkSessionJob {
	if sj == nil {
		return nil
	}
	out := new(FlinkSessionJob)
	sj.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies sj into out.
func (sj *FlinkSessionJob) DeepCopyInto(out *FlinkSessionJob) {
	*out = *sj
	sj.Metadata.DeepCopyInto(&out.Metadata)
	out.Spec.RestartNonce = ptrCopy(sj.Spec.RestartNonce)
	out.Spec.FlinkConfiguration = maps.Clone(sj.Spec.FlinkConfiguration)
	out.Spec.Job = ptrCopy(sj.Spec.Job)
}

// DeepCopyObject implements runtime.Object.
func (sj *FlinkSessionJob) DeepCopyObject() runtime.Object { return sj.DeepCopy() }

func (s *ContainerState) deepCopyInto(out *ContainerState) {
	out.Running = ptrCopy(s.Running)
	out.Waiting = ptrCopy(s.Waiting)
	out.Terminated = nil
	if s.Terminated != nil {
		t := *s.Terminated
		s.Terminated.FinishedAt.DeepCopyInto(&t.FinishedAt)
		out.Terminated = &t
	}
}

// DeepCopy copies p.
func (p *Pod) DeepCopy() *Pod {
	if p == nil {
		return nil
	}
	out := new(Pod)
	p.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies p into out.
func (p *Pod) DeepCopyInto(out *Pod) {
	*out = *p
	p.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if p.Spec.Containers != nil {
		out.Spec.Containers = make([]Container, len(p.Spec.Containers))
		for i, c := range p.Spec.Containers {
			if c.Resources.Requests != nil {
				req := make(map[string]resource.Quantity, len(c.Resources.Requests))
				for k, q := range c.Resources.Requests {
					req[k] = q.DeepCopy()
				}
				c.Resources.Requests = req
			}
			out.Spec.Containers[i] = c
		}
	}
	out.Status.Conditions = slices.Clone(p.Status.Conditions)
	out.Status.ContainerStatuses = copyStatuses(p.Status.ContainerStatuses)
	out.Status.InitContainerStatuses = copyStatuses(p.Status.InitContainerStatuses)
}

// DeepCopyObject implements runtime.Object.
func (p *Pod) DeepCopyObject() runtime.Object { return p.DeepCopy() }

func copyItems[T any](items []T, cp func(in, out *T)) []T {
	if items == nil {
		return nil
	}
	out := make([]T, len(items))
	for i := range items {
		cp(&items[i], &out[i])
	}
	return out
}

// DeepCopyObject implements runtime.Object.
func (l *FlinkDeploymentList) DeepCopyObject() runtime.Object {
	out := &FlinkDeploymentList{TypeMeta: l.TypeMeta}
	l.DeepCopyInto(&out.ListMeta)
	out.Items = copyItems(l.Items, (*FlinkDeployment).DeepCopyInto)
	return out
}

// DeepCopyObject implements runtime.Object.
func (l *FlinkSessionJobList) DeepCopyObject() runtime.Object {
	out := &FlinkSessionJobList{TypeMeta: l.TypeMeta}
	l.DeepCopyInto(&out.ListMeta)
	out.Items = copyItems(l.Items, (*FlinkSessionJob).DeepCopyInto)
	return out
}

// DeepCopyObject implements runtime.Object.
func (l *PodList) DeepCopyObject() runtime.Object {
	out := &PodList{TypeMeta: l.TypeMeta}
	l.DeepCopyInto(&out.ListMeta)
	out.Items = copyItems(l.Items, (*Pod).DeepCopyInto)
	return out
}

func copyStatuses(in []ContainerStatus) []ContainerStatus {
	if in == nil {
		return nil
	}
	out := make([]ContainerStatus, len(in))
	for i := range in {
		out[i] = in[i]
		in[i].State.deepCopyInto(&out[i].State)
		in[i].LastState.deepCopyInto(&out[i].LastState)
	}
	return out
}
