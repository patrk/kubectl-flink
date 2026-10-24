package fleet_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
)

// BenchmarkRederive measures what -w does on every coalesced change: convert all cached objects of 1,500 jobs
// and 3,000 pods into jobs and assess them.
func BenchmarkRederive(b *testing.B) {
	var fd kube.FlinkDeployment
	var pods kube.PodList
	for path, v := range map[string]any{fdFixture: &fd, podsFixture: &pods} {
		data, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		if err := json.Unmarshal(data, v); err != nil {
			b.Fatal(err)
		}
	}
	snap := &kube.Snapshot{}
	for i := range 1500 {
		d := *fd.DeepCopy()
		d.Name = fmt.Sprintf("job-%d", i)
		snap.Deployments = append(snap.Deployments, d)
		for _, component := range []string{kube.ComponentJM, kube.ComponentTM} {
			p := *pods.Items[0].DeepCopy()
			p.Labels = map[string]string{kube.LabelApp: d.Name, kube.LabelComponent: component}
			snap.Pods = append(snap.Pods, p)
		}
	}
	now, th := time.Now(), model.DefaultThresholds()
	b.ReportAllocs()
	for b.Loop() {
		jobs, _ := fleet.Jobs("dev", snap)
		rows := fleet.Rows([]fleet.Scope{{Jobs: jobs}}, now, th)
		if err := fleet.Sort(rows, "health"); err != nil {
			b.Fatal(err)
		}
	}
}
