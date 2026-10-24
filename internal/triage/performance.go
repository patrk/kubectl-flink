package triage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/patrk/kubectl-flink/internal/flink"
)

// VertexMetrics are one vertex's throughput and time split, aggregated over its subtasks.
type VertexMetrics struct {
	ID      string                  `json:"id"`
	Name    string                  `json:"name"`
	Source  bool                    `json:"source,omitempty"`
	Sink    bool                    `json:"sink,omitempty"`
	Metrics map[string]flink.Metric `json:"metrics"`
}

// Standard task metrics every vertex has, and the optional source backlog, which carries an operator prefix.
const (
	MetricBusy        = "busyTimeMsPerSecond"
	MetricBackPressed = "backPressuredTimeMsPerSecond"
	MetricIdle        = "idleTimeMsPerSecond"
	MetricIn          = "numRecordsInPerSecond"
	MetricOut         = "numRecordsOutPerSecond"
	metricPending     = ".pendingRecords"
)

var standardMetrics = []string{MetricBusy, MetricBackPressed, MetricIdle, MetricIn, MetricOut}

// maxVertices bounds the performance layer: wider jobs are left to the Flink UI rather than slowing why down.
const maxVertices = 50

// parallelMetrics bounds the metric requests in flight.
const parallelMetrics = 8

// collectMetrics reads the metrics of every vertex of a running job. A source is also asked for its
// pendingRecords, but only when it lists one: asking for a metric a vertex lacks empties the whole answer.
func collectMetrics(ctx context.Context, c *flink.Client, job *flink.Job) ([]VertexMetrics, error) {
	if job.State != "RUNNING" {
		return nil, nil // only a running job has meaningful rates
	}
	if len(job.Vertices) > maxVertices {
		return nil, fmt.Errorf("%w: %d vertices, more than %d", errNotAsked, len(job.Vertices), maxVertices)
	}
	sources, sinks := map[string]bool{}, map[string]bool{}
	for _, id := range job.Sources() {
		sources[id] = true
	}
	for _, id := range job.Sinks() {
		sinks[id] = true
	}
	out := make([]VertexMetrics, len(job.Vertices))
	errs := make([]error, len(job.Vertices))
	sem := make(chan struct{}, parallelMetrics)
	var wg sync.WaitGroup
	for i := range job.Vertices {
		v := &job.Vertices[i]
		out[i] = VertexMetrics{ID: v.ID, Name: v.Name, Source: sources[v.ID], Sink: sinks[v.ID]}
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			names := standardMetrics
			if sources[v.ID] {
				all, err := c.MetricNames(ctx, job.JID, v.ID)
				if err != nil {
					errs[i] = err
					return
				}
				for _, n := range all {
					if strings.HasSuffix(n, metricPending) {
						names = append(names[:len(names):len(names)], n)
					}
				}
			}
			m, err := c.Metrics(ctx, job.JID, v.ID, names...)
			out[i].Metrics, errs[i] = m, err
		})
	}
	wg.Wait()
	// A vertex whose request failed is left out, so that missing numbers never read as zero rates.
	var got []VertexMetrics
	var failed []error
	for i := range out {
		if errs[i] != nil {
			failed = append(failed, fmt.Errorf("%s: %w", out[i].Name, errs[i]))
			continue
		}
		got = append(got, out[i])
	}
	return got, errors.Join(failed...)
}

// Pending is the summed backlog of a source vertex, and whether it reports one.
func (v *VertexMetrics) Pending() (float64, bool) {
	sum, ok := 0.0, false
	for id, m := range v.Metrics {
		if strings.HasSuffix(id, metricPending) {
			sum += m.Sum
			ok = true
		}
	}
	return sum, ok
}

// Thresholds of the performance signals, in milliseconds per second of a subtask, as the Flink UI uses them.
const (
	backpressureHigh = 500 // a subtask waiting on its output half the time or more
	busyHigh         = 900 // a subtask that never waits for input
	minSourceRate    = 1   // records per second the sources must emit before a silent sink is notable
)

// performanceSignals reports only what is notable: backpressure, the busy vertex behind it, and a sink that
// receives nothing while the sources emit.
func performanceSignals(job *flink.Job, vms []VertexMetrics, add func(kind string, layer Layer, text string)) {
	byID := map[string]*VertexMetrics{}
	sourceRate := 0.0
	for i := range vms {
		byID[vms[i].ID] = &vms[i]
		if vms[i].Source {
			sourceRate += vms[i].Metrics[MetricOut].Sum
		}
	}
	for i := range vms {
		v := &vms[i]
		bp := v.Metrics[MetricBackPressed].Max
		if bp < backpressureHigh {
			continue
		}
		add(SigBackpressure, Performance, fmt.Sprintf("%s: backpressured %.0f ms/s", v.Name, bp))
		if d := bottleneck(job, byID, v.ID); d != nil {
			add(SigBottleneck, Performance, fmt.Sprintf("%s: busy %.0f ms/s", d.Name, d.Metrics[MetricBusy].Max))
		}
	}
	if sourceRate < minSourceRate {
		return
	}
	for i := range vms {
		if v := &vms[i]; v.Sink && !v.Source && v.Metrics != nil && v.Metrics[MetricIn].Sum == 0 {
			add(SigSinkIdle, Performance, fmt.Sprintf("%s receives nothing while the sources emit %.0f records/s", v.Name, sourceRate))
		}
	}
}

// bottleneck is the nearest busy vertex downstream of a backpressured one, searching every branch breadth first.
func bottleneck(job *flink.Job, byID map[string]*VertexMetrics, from string) *VertexMetrics {
	seen := map[string]bool{from: true}
	queue := job.Downstream(from)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		if d := byID[id]; d != nil && d.Metrics[MetricBusy].Max >= busyHigh {
			return d
		}
		queue = append(queue, job.Downstream(id)...)
	}
	return nil
}
