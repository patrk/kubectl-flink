package tui

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/patrk/kubectl-flink/internal/render"
)

// boxWidth is the inside width of an operator's box.
const boxWidth = 30

// graphView draws the job graph as the Flink web UI does: a box per operator, coloured by what fills its time,
// in layers from the sources, left to right when they fit the width, else top down. Each box lists the operators
// it feeds, so every edge is exact however the boxes are placed.
func graphView(g *render.JobGraph, width int, st *styles) string {
	if g.Missing != "" {
		return "No job graph: " + g.Missing + "."
	}
	head := fmt.Sprintf("job %s, %d operators; %s", g.State, len(g.Nodes), g.Note)
	var layers [][]string
	for i := range g.Nodes {
		n := &g.Nodes[i]
		for len(layers) <= n.Layer {
			layers = append(layers, nil)
		}
		layers[n.Layer] = append(layers[n.Layer], box(n, st))
	}
	cols := make([]string, 0, 2*len(layers))
	for i, l := range layers {
		col := lipgloss.JoinVertical(lipgloss.Left, l...)
		if i > 0 {
			arrow := lipgloss.PlaceVertical(lipgloss.Height(col), lipgloss.Center, " ─▶ ")
			cols = append(cols, arrow)
		}
		cols = append(cols, col)
	}
	across := lipgloss.JoinHorizontal(lipgloss.Center, cols...)
	if lipgloss.Width(across) <= width {
		return head + "\n\n" + across
	}
	// Top down: each layer's boxes side by side, wrapped to the width, an arrow between layers.
	rows := make([]string, 0, 2*len(layers))
	for i, l := range layers {
		if i > 0 {
			rows = append(rows, "  ▼")
		}
		rows = append(rows, flow(l, width))
	}
	return head + "\n\n" + strings.Join(rows, "\n")
}

// box is one operator: its number and name, parallelism and status, the time split bar, and its rates.
func box(n *render.GraphNode, st *styles) string {
	style := st.box.Width(boxWidth + 4).BorderForeground(st.state[n.State].GetForeground())
	title := fmt.Sprintf("[%d] %s", n.Index, n.Name)
	if n.Bottleneck {
		style = style.Border(lipgloss.ThickBorder())
	}
	lines := []string{st.state[n.State].Bold(true).Render(wrapWords(title, boxWidth)), fmt.Sprintf("×%d %s", n.Parallelism, n.Status)}
	if n.Bottleneck {
		lines = append(lines, st.state[render.Busy].Render("◀ bottleneck"))
	}
	if n.Metrics {
		paint := func(state, s string) string { return st.state[state].Render(s) }
		lines = append(lines,
			render.Bar(n, boxWidth, paint),
			fmt.Sprintf("busy %.0f · bp %.0f %s · idle %.0f", n.Busy, n.Pressed, n.Backpressure, n.Idle),
			n.Rates())
		if n.HasPending {
			lines = append(lines, fmt.Sprintf("%.0f pending", n.Pending))
		}
	}
	if len(n.Downstream) > 0 {
		to := make([]string, len(n.Downstream))
		for i, d := range n.Downstream {
			to[i] = fmt.Sprintf("[%d]", d)
		}
		lines = append(lines, "→ "+strings.Join(to, " "))
	}
	return style.Render(strings.Join(lines, "\n"))
}

// flow places boxes side by side, starting a new row when the next would pass width.
func flow(boxes []string, width int) string {
	var rows []string
	row := make([]string, 0, len(boxes))
	used := 0
	for _, b := range boxes {
		w := lipgloss.Width(b)
		if len(row) > 0 && used+w > width {
			rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, row...))
			row, used = nil, 0
		}
		row = append(row, b)
		used += w
	}
	if len(row) > 0 {
		rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, row...))
	}
	return strings.Join(rows, "\n")
}

// wrapWords wraps text at spaces to width, cutting a word longer than width.
func wrapWords(text string, width int) string {
	var lines []string
	line := ""
	for _, w := range strings.Fields(text) {
		w = ansi.Truncate(w, width, "…")
		if line != "" && ansi.StringWidth(line)+1+ansi.StringWidth(w) > width {
			lines = append(lines, line)
			line = ""
		}
		if line != "" {
			line += " "
		}
		line += w
	}
	return strings.Join(append(lines, line), "\n")
}
