package model

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrk/kubectl-flink/internal/kube"
)

func TestSummarisePodsFixtures(t *testing.T) {
	const dir = "../../testdata/fixtures/"
	for _, v := range []string{"operator-1.15.0-flink-1.20", "operator-1.15.0-flink-2.2", "operator-1.16.1-flink-1.20", "operator-1.16.1-flink-2.2"} {
		s := SummarisePods(loadPods(t, dir+v+"/pods.json"))
		cp := s["default/basic"]
		if cp == nil {
			t.Fatalf("%s: no default/basic cluster in %v", v, s)
		}
		if cp.JM.Total != 1 || cp.JM.Ready != 1 || cp.TM.Total != 1 || cp.TM.Ready != 1 || cp.JM.Waiting != "" {
			t.Errorf("%s: %+v", v, cp)
		}
		if cp.CPU.IsZero() || cp.Memory.IsZero() {
			t.Errorf("%s: requests cpu %s memory %s", v, cp.CPU.String(), cp.Memory.String())
		}
	}

	bad := SummarisePods(loadPods(t, dir+"scenarios-operator-1.16.1-flink-2.2/bad-image/pods.json"))
	for key, cp := range bad {
		if cp.JM.Ready != 0 || !strings.HasPrefix(cp.JM.Waiting, "ImagePull") && !strings.HasPrefix(cp.JM.Waiting, "ErrImagePull") {
			t.Errorf("bad-image %s: JM %+v", key, cp.JM)
		}
	}
	crash := SummarisePods(loadPods(t, dir+"scenarios-operator-1.16.1-flink-2.2/missing-jar/pods.json"))
	for key, cp := range crash {
		if cp.JM.Restarts == 0 || cp.JM.LastTermination == "" {
			t.Errorf("missing-jar %s: JM %+v", key, cp.JM)
		}
	}
	if len(bad) == 0 || len(crash) == 0 {
		t.Error("scenario pods not found")
	}
}

func TestAttachPods(t *testing.T) {
	pods := map[string]*ClusterPods{"ns/session": {TM: Component{Total: 3}}, "ns/app": {TM: Component{Total: 1}}}
	jobs := []Job{
		{Kind: KindDeployment, Namespace: "ns", Name: "app"},
		{Kind: KindSessionJob, Namespace: "ns", Name: "sj", DeploymentName: "session"},
		{Kind: KindDeployment, Namespace: "ns", Name: "gone"},
		{Kind: KindDeployment, Namespace: "unread", Name: "app"},
	}
	AttachPods(jobs, pods, func(ns string) bool { return ns == "ns" })
	if jobs[0].Pods.TM.Total != 1 || jobs[1].Pods != nil || jobs[2].Pods == nil || jobs[2].Pods.TM.Total != 0 || jobs[3].Pods != nil {
		t.Errorf("%+v %+v %+v %+v", jobs[0].Pods, jobs[1].Pods, jobs[2].Pods, jobs[3].Pods)
	}
}

func pod(component, phase string, ready bool, waiting string) kube.Pod {
	p := kube.Pod{}
	p.Namespace, p.Name = "ns", component+"-"+phase
	p.Labels = map[string]string{kube.LabelApp: "app", kube.LabelComponent: component}
	p.Status.Phase = phase
	status := "False"
	if ready {
		status = "True"
	}
	p.Status.Conditions = []kube.PodCondition{{Type: "Ready", Status: status}}
	cs := kube.ContainerStatus{Name: "flink-main-container", Ready: ready}
	if waiting != "" {
		cs.State.Waiting = &kube.ContainerWaiting{Reason: waiting}
	}
	p.Status.ContainerStatuses = []kube.ContainerStatus{cs}
	return p
}

func TestSummarisePodsSkipsLeftoversAndStartup(t *testing.T) {
	deleting := pod(kube.ComponentTM, "Running", true, "")
	deleting.Name = "deleting"
	deleting.DeletionTimestamp = &metav1.Time{}
	s := SummarisePods([]kube.Pod{
		pod(kube.ComponentTM, "Running", true, ""),
		pod(kube.ComponentTM, "Failed", false, ""),    // evicted
		pod(kube.ComponentTM, "Succeeded", false, ""), // completed
		deleting,
		pod(kube.ComponentTM, "Pending", false, "ContainerCreating"),
	})["ns/app"]
	if s.TM.Total != 2 || s.TM.Ready != 1 || s.TM.Waiting != "" {
		t.Errorf("TM %+v; want 2 pods, 1 ready, no failure reason", s.TM)
	}
	s = SummarisePods([]kube.Pod{pod(kube.ComponentTM, "Pending", false, "ErrImagePull")})["ns/app"]
	if s.TM.Waiting != "ErrImagePull (taskmanager-Pending)" {
		t.Errorf("waiting %q", s.TM.Waiting)
	}
}
