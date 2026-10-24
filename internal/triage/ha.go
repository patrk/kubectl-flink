package triage

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
)

// HAState is what Kubernetes HA keeps for a cluster: the job plans in the cluster ConfigMap, and per job a
// ConfigMap with its checkpoint pointers. The job result store lives in the HA storage directory and is not read.
type HAState struct {
	ConfigMaps []string `json:"configMaps"`
	// Plans are the job IDs with a stored job plan (executionPlan-<id> on Flink 2, jobGraph-<id> before).
	Plans []string `json:"plans"`
	// Checkpoints are the job IDs with a checkpoint ConfigMap.
	Checkpoints []string `json:"checkpoints"`
}

// HAEnabled reports whether the job's configuration turns Kubernetes or ZooKeeper HA on.
func HAEnabled(j *model.Job) bool {
	for _, key := range []string{"high-availability.type", "high-availability"} {
		if v := strings.ToLower(j.Config[key]); v != "" && v != "none" {
			return true
		}
	}
	return false
}

// collectHA reads the cluster's HA ConfigMaps by key.
func collectHA(ctx context.Context, kc *kube.Client, namespace, cluster string) (*HAState, error) {
	obj, err := kc.List(ctx, kube.ConfigMaps, namespace, kube.LabelApp+"="+cluster)
	if err != nil {
		return nil, err
	}
	st := &HAState{}
	items := obj.(*kube.ConfigMapList).Items
	for i := range items {
		cm := &items[i]
		if !strings.HasPrefix(cm.Name, cluster+"-") || !strings.HasSuffix(cm.Name, "-config-map") {
			continue
		}
		st.ConfigMaps = append(st.ConfigMaps, cm.Name)
		if mid := strings.TrimSuffix(strings.TrimPrefix(cm.Name, cluster+"-"), "-config-map"); mid != "cluster" {
			st.Checkpoints = append(st.Checkpoints, mid)
			continue
		}
		for _, k := range cm.Keys {
			for _, prefix := range []string{"executionPlan-", "jobGraph-"} {
				if id, ok := strings.CutPrefix(k, prefix); ok {
					st.Plans = append(st.Plans, id)
				}
			}
		}
	}
	slices.Sort(st.Plans)
	slices.Sort(st.Checkpoints)
	return st, nil
}

// haSignals reports HA state that does not belong to the current job: on restart, the JobManager recovers
// every stored job plan, so a leftover one runs an old job next to, or instead of, the current one. Only an
// application cluster with a known job can tell; a session cluster runs several jobs by design.
func haSignals(st *HAState, j *model.Job, add func(kind string, layer Layer, text string)) {
	jobID := j.JobID
	if j.Kind != model.KindDeployment || j.SessionCluster || jobID == "" {
		return
	}
	for _, id := range st.Plans {
		if id != jobID {
			add(SigHALeftover, HA, fmt.Sprintf("job plan of %s, not the current job %s", id, jobID))
		}
	}
	for _, id := range st.Checkpoints {
		if id != jobID && !slices.Contains(st.Plans, id) {
			add(SigHALeftover, HA, fmt.Sprintf("checkpoint pointers of %s, not the current job %s", id, jobID))
		}
	}
}
