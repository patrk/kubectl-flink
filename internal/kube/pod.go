package kube

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Labels Flink puts on JobManager and TaskManager pods, in native mode and in the operator's standalone mode.
const (
	LabelType      = "type"
	LabelApp       = "app"
	LabelComponent = "component"
	// FlinkPodSelector selects the pods of every Flink cluster: native mode (Flink's own labels) and standalone
	// mode (the operator's StandaloneKubernetesUtils), both with the app and component labels.
	FlinkPodSelector = LabelType + " in (flink-native-kubernetes,flink-standalone-kubernetes)"
	ComponentJM      = "jobmanager"
	ComponentTM      = "taskmanager"
)

// ClusterKey identifies the Flink cluster a pod belongs to: its namespace and the app label, which is the
// FlinkDeployment's name.
func ClusterKey(namespace, app string) string { return namespace + "/" + app }

// Pod is the subset of a core/v1 Pod the tool reads: phase, readiness, restarts, the last failure and requests.
// Managed fields, volumes, env and the rest of the spec are skipped by the decoder.
type Pod struct {
	metav1.TypeMeta `json:",inline"`
	ObjectMeta      `json:"metadata"`
	Spec            PodSpec   `json:"spec"`
	Status          PodStatus `json:"status"`
}

// PodSpec is the subset of the pod spec the tool reads.
type PodSpec struct {
	NodeName   string      `json:"nodeName,omitempty"`
	Containers []Container `json:"containers,omitempty"`
}

// Container keeps a container's name and resource requests.
type Container struct {
	Name      string    `json:"name"`
	Resources Resources `json:"resources"`
}

// Resources keeps a container's requests.
type Resources struct {
	Requests map[string]resource.Quantity `json:"requests,omitempty"`
}

// PodStatus is the subset of the pod status the tool reads.
type PodStatus struct {
	Phase             string            `json:"phase,omitempty"`
	Reason            string            `json:"reason,omitempty"`
	Conditions        []PodCondition    `json:"conditions,omitempty"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
}

// PodCondition keeps when a pod condition, such as Ready, last changed.
type PodCondition struct {
	Type               string      `json:"type"`
	Status             string      `json:"status"`
	Reason             string      `json:"reason,omitempty"`  // such as Unschedulable
	Message            string      `json:"message,omitempty"` // such as "0/3 nodes are available: ..."
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
}

// ContainerStatus is the subset of a container status the tool reads.
type ContainerStatus struct {
	Name         string         `json:"name"`
	Ready        bool           `json:"ready"`
	RestartCount int32          `json:"restartCount"`
	State        ContainerState `json:"state"`
	LastState    ContainerState `json:"lastState"`
}

// ContainerState keeps why a container waits or how it last terminated.
type ContainerState struct {
	Running    *ContainerRunning    `json:"running,omitempty"`
	Waiting    *ContainerWaiting    `json:"waiting,omitempty"`
	Terminated *ContainerTerminated `json:"terminated,omitempty"`
}

// ContainerRunning is a running container.
type ContainerRunning struct {
	StartedAt metav1.Time `json:"startedAt"`
}

// ContainerWaiting is why a container is not running, such as ImagePullBackOff or CrashLoopBackOff.
type ContainerWaiting struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ContainerTerminated is a container's termination.
type ContainerTerminated struct {
	Reason     string      `json:"reason,omitempty"`
	Message    string      `json:"message,omitempty"`
	ExitCode   int32       `json:"exitCode"`
	FinishedAt metav1.Time `json:"finishedAt"`
}

// Ready reports whether the pod is running and its Ready condition is true.
func (p *Pod) Ready() bool {
	if p.Status.Phase != "Running" || p.DeletionTimestamp != nil {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

// MainContainer is the name of the Flink container in JobManager and TaskManager pods.
const MainContainer = "flink-main-container"
