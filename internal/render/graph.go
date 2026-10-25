package render

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// What fills an operator's time, which the Flink web UI shows as its colour: busy red, back-pressured black,
// idle blue.
const (
	Busy    = "busy"
	Pressed = "back-pressured"
	Idle    = "idle"
)

// ANSI colours of the text graph; dark grey stands for the UI's black, to stay visible on a dark terminal.
var stateColors = map[string]string{Busy: "31", Pressed: "90", Idle: "34"}

// barWidth is the width of the time split bar.
const barWidth = 20

// JobGraph is a job's operators from the sources down, for the graph command and top's graph tab.
type JobGraph struct {
	Job JobRef `json:"job"`
	// Missing says why there is no graph; Nodes is then empty.
	Missing string `json:"missing,omitempty"`
	// State is the job's state; Note says how to read the rates, or why there are none.
	State string      `json:"state,omitempty"`
	Note  string      `json:"note,omitempty"`
	Nodes []GraphNode `json:"operators,omitempty"`
}

// GraphNode is one operator chain.
type GraphNode struct {
	// Index numbers the nodes from 1 in order; Layer is the longest path from a source, from 0.
	Index       int    `json:"index"`
	Layer       int    `json:"layer"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Parallelism int    `json:"parallelism"`
	Status      string `json:"status"`
	// Metrics is false when the operator's metrics were not read; the time split and rates are then zero.
	Metrics bool `json:"metrics"`
	// Busy and Pressed are the ms/s of the busiest and the most back-pressured subtask, Idle of the least idle one,
	// so that together they describe the worst subtask; In and Out are records/s over all subtasks.
	Busy float64 `json:"busyMsPerSecond"`
	// BusyLeast is the busy ms/s of the least busy subtask; Skewed marks an operator whose busiest subtask is
	// saturated while another works much less.
	BusyLeast  float64 `json:"leastBusyMsPerSecond,omitempty"`
	Skewed     bool    `json:"skewed,omitempty"`
	Pressed    float64 `json:"backPressuredMsPerSecond"`
	Idle       float64 `json:"idleMsPerSecond"`
	In         float64 `json:"recordsInPerSecond"`
	Out        float64 `json:"recordsOutPerSecond"`
	Pending    float64 `json:"pendingRecords,omitempty"`
	HasPending bool    `json:"-"`
	// State is what fills most of the operator's time; Backpressure is Flink's OK, LOW or HIGH.
	State        string `json:"state,omitempty"`
	Backpressure string `json:"backpressure,omitempty"`
	// Bottleneck marks the operator why names as the busy one behind back pressure.
	Bottleneck bool `json:"bottleneck,omitempty"`
	// Downstream are the Index values of the operators that read from this one.
	Downstream []int `json:"downstream,omitempty"`
}

// NewJobGraph builds the graph from why's evidence.
func NewJobGraph(ev *triage.Evidence) *JobGraph {
	g := &JobGraph{Job: NewJobRef(ev.Job)}
	if ev.REST == nil || ev.REST.Job == nil {
		g.Missing = "the JobManager was not asked"
		for _, gap := range ev.Gaps {
			if gap.What == "JobManager REST API" {
				g.Missing = gap.Why
			}
		}
		return g
	}
	job := ev.REST.Job
	g.State = job.State
	switch {
	case job.State != "RUNNING":
		g.Note = "rates only while it runs"
	case len(ev.Metrics) == 0:
		g.Note = "metrics not read"
		for _, gap := range ev.Gaps {
			if gap.What == "vertex metrics" {
				g.Note = "metrics not read: " + gap.Why
			}
		}
	case !triage.Settled(job):
		g.Note = "rates average the last minute and are not judged in the first " + triage.Short(triage.Warmup)
	default:
		g.Note = "time split of the worst subtask, rates summed"
	}
	byID := map[string]*triage.VertexMetrics{}
	for i := range ev.Metrics {
		byID[ev.Metrics[i].ID] = &ev.Metrics[i]
	}
	bottlenecks := triage.Bottlenecks(job, ev.Metrics)
	order, layers := planOrder(job)
	// Only plan nodes that are vertices of the job are drawn, numbered in order.
	order = slices.DeleteFunc(order, func(id string) bool { return job.Vertex(id) == nil })
	index := map[string]int{}
	for i, id := range order {
		index[id] = i + 1
	}
	for _, id := range order {
		v := job.Vertex(id)
		n := GraphNode{Index: index[id], Layer: layers[id], ID: id, Name: v.Name, Parallelism: v.Parallelism,
			Status: v.Status, Bottleneck: bottlenecks[id]}
		if vm := byID[id]; vm != nil && vm.Metrics != nil {
			m := vm.Metrics
			n.Metrics = true
			n.Busy, n.Pressed, n.Idle = triage.WorstSplit(m)
			if b := m[triage.MetricBusy]; b.HasMin {
				n.BusyLeast = b.Min
			}
			n.Skewed = triage.Skewed(job, vm)
			n.In, n.Out = m[triage.MetricIn].Sum, m[triage.MetricOut].Sum
			n.Pending, n.HasPending = vm.Pending()
			n.State, n.Backpressure = dominant(n.Busy, n.Pressed, n.Idle), triage.BackpressureLevel(n.Pressed)
		}
		for _, d := range job.Downstream(id) {
			if index[d] > 0 {
				n.Downstream = append(n.Downstream, index[d])
			}
		}
		g.Nodes = append(g.Nodes, n)
	}
	return g
}

// Graph prints the job's operators from the sources down, each with its parallelism, the time split of its worst
// subtask and its rates, coloured as the Flink web UI colours them when color is set.
func Graph(out io.Writer, ev *triage.Evidence, color bool) error {
	g := NewJobGraph(ev)
	w := &errWriter{w: out}
	w.printf("%s\n", g.Job)
	if g.Missing != "" {
		w.printf("\nNo job graph: %s.\n", g.Missing)
		return w.err
	}
	w.printf("job %s, %d operators; %s\n", g.State, len(g.Nodes), g.Note)
	paint := func(state, s string) string {
		if !color || stateColors[state] == "" {
			return s
		}
		return "\x1b[" + stateColors[state] + "m" + s + "\x1b[0m"
	}
	for i := range g.Nodes {
		n := &g.Nodes[i]
		w.printf("\n[%d] %s  ×%d  %s", n.Index, paint(n.State, n.Name), n.Parallelism, n.Status)
		if n.Bottleneck {
			w.printf("  %s", paint(Busy, "◀ bottleneck"))
		}
		if n.Skewed {
			w.printf("  %s", paint(Busy, fmt.Sprintf("◀ skewed: subtasks busy %.0f to %.0f ms/s", n.BusyLeast, n.Busy)))
		}
		w.printf("\n")
		if n.Metrics {
			w.printf("    %s  %s · %s", Bar(n, barWidth, paint), n.TimeSplit(), n.Rates())
			if n.HasPending {
				w.printf(" · %.0f pending", n.Pending)
			}
			w.printf("\n")
		}
		for _, d := range n.Downstream {
			w.printf("    └─▶ [%d] %s\n", d, g.Nodes[d-1].Name)
		}
	}
	return w.err
}

// TimeSplit is "busy 10, back-pressured 800 (HIGH), idle 190 ms/s".
func (n *GraphNode) TimeSplit() string {
	return fmt.Sprintf("busy %.0f, back-pressured %.0f (%s), idle %.0f ms/s", n.Busy, n.Pressed, n.Backpressure, n.Idle)
}

// Rates is "in 6.2k/s, out 900/s".
func (n *GraphNode) Rates() string { return "in " + Rate(n.In) + ", out " + Rate(n.Out) }

// planOrder is the vertices from the sources down, every vertex after all it reads from, ties in plan order, with
// each vertex's layer: its longest path from a source. Vertices missing from the plan follow in the job's order.
func planOrder(job *flink.Job) (order []string, layers map[string]int) {
	inputs := map[string]int{}
	for _, n := range job.Plan.Nodes {
		inputs[n.ID] = len(n.Inputs)
	}
	layers = map[string]int{}
	done := map[string]bool{}
	ready := job.Sources()
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		if done[id] {
			continue
		}
		done[id] = true
		order = append(order, id)
		for _, d := range job.Downstream(id) {
			layers[d] = max(layers[d], layers[id]+1)
			inputs[d]--
			if inputs[d] == 0 {
				ready = append(ready, d)
			}
		}
	}
	for _, v := range job.Vertices {
		if !done[v.ID] {
			order = append(order, v.ID)
		}
	}
	return order, layers
}

// dominant is what fills most of an operator's time; none when no time was measured.
func dominant(busy, pressed, idle float64) string {
	switch {
	case busy+pressed+idle <= 0:
		return ""
	case pressed >= busy && pressed >= idle:
		return Pressed
	case busy >= idle:
		return Busy
	}
	return Idle
}

// Bar splits width cells into busy (#), back-pressured (=) and idle (.), each passed through paint with its state.
func Bar(n *GraphNode, width int, paint func(state, s string) string) string {
	total := n.Busy + n.Pressed + n.Idle
	if total <= 0 {
		return strings.Repeat(" ", width)
	}
	b := int(n.Busy/total*float64(width) + 0.5)
	p := min(int(n.Pressed/total*float64(width)+0.5), width-b)
	return paint(Busy, strings.Repeat("#", b)) + paint(Pressed, strings.Repeat("=", p)) + paint(Idle, strings.Repeat(".", width-b-p))
}

// Rate is records per second, shortened: 950/s, 1.2k/s, 12k/s, 4.1M/s.
func Rate(r float64) string {
	switch {
	case r >= 1e6:
		return strconv.FormatFloat(r/1e6, 'f', 1, 64) + "M/s"
	case r >= 1e4:
		return strconv.FormatFloat(r/1e3, 'f', 0, 64) + "k/s"
	case r >= 999.5: // what would print as 1000/s
		return strconv.FormatFloat(r/1e3, 'f', 1, 64) + "k/s"
	}
	return fmt.Sprintf("%.0f/s", r)
}
