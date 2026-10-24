//go:build e2e

package flink_test

import (
	"context"
	"os"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericclioptions"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
)

// TestLivePortForward forwards to the e2e job's JobManager in process and reads its REST API (make e2e).
func TestLivePortForward(t *testing.T) {
	kctx := os.Getenv("KUBE_CONTEXT")
	if kctx == "" {
		t.Skip("KUBE_CONTEXT not set")
	}
	flags := genericclioptions.NewConfigFlags(false)
	flags.Context = &kctx
	cs, err := kube.NewContexts(flags)
	if err != nil {
		t.Fatal(err)
	}
	cfg, ns, err := cs.Config("")
	if err != nil {
		t.Fatal(err)
	}
	c, err := kube.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := flink.NewPool(flink.NewForwarder(cfg), func(ctx context.Context, ns, cluster string) ([]string, error) {
		pods, err := c.ComponentPods(ctx, ns, cluster, kube.ComponentJM)
		var names []string
		for i := range pods {
			if pods[i].Ready() {
				names = append(names, pods[i].Name)
			}
		}
		return names, err
	}, 4)
	defer pool.Close()

	for range 2 { // the second round reuses the forward
		err = pool.Do(ctx, ns, "basic", func(fc *flink.Client) error {
			cfg, err := fc.Config(ctx)
			if err != nil {
				return err
			}
			jobs, err := fc.Jobs(ctx)
			if err != nil {
				return err
			}
			if cfg.FlinkVersion == "" || len(jobs) == 0 || jobs[0].State != "RUNNING" {
				t.Errorf("config %+v, jobs %+v", cfg, jobs)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
