package tui

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/patrk/kubectl-flink/internal/fleet"
)

// choice is a namespace or context with its number of jobs.
type choice struct {
	name string
	jobs int
}

// picker chooses a namespace or context by typing part of its name; the first entry stands for all of them.
type picker struct {
	title   string
	choices []choice
	shown   []choice
	cursor  int
}

const pickerRows = 12

func (m *Model) openPicker(title string, choices []choice, current string) {
	m.mode = modePicker
	m.picker = picker{title: title, choices: append([]choice{{name: "", jobs: -1}}, choices...)}
	m.prompt("> ", "", nil)
	m.picker.filter("")
	if i := slices.IndexFunc(m.picker.shown, func(c choice) bool { return c.name == current }); i >= 0 {
		m.picker.cursor = i
	}
}

func (p *picker) filter(text string) {
	p.shown = p.shown[:0]
	for _, c := range p.choices {
		if c.name == "" || strings.Contains(c.name, text) {
			p.shown = append(p.shown, c)
		}
	}
	// With text typed, the first match is a better default than "all".
	p.cursor = 0
	if text != "" && len(p.shown) > 1 {
		p.cursor = 1
	}
}

func (m *Model) pickerKey(msg tea.KeyPressMsg) tea.Cmd {
	p := &m.picker
	switch {
	case key.Matches(msg, m.keys.back):
		m.mode = modeList
		m.input.Blur()
		return nil
	case key.Matches(msg, m.keys.enter):
		m.mode = modeList
		m.input.Blur()
		// Only "all" is left when nothing matches; Enter then keeps the filter rather than dropping it.
		if len(p.shown) == 1 && m.input.Value() != "" {
			return nil
		}
		if p.cursor < len(p.shown) {
			if p.title == "context" {
				m.context = p.shown[p.cursor].name
			} else {
				m.namespace = p.shown[p.cursor].name
			}
			m.apply()
		}
		return nil
	case msg.String() == "up":
		p.cursor = max(0, p.cursor-1)
		return nil
	case msg.String() == "down":
		p.cursor = min(len(p.shown)-1, p.cursor+1)
		return nil
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	p.filter(m.input.Value())
	return cmd
}

func (p *picker) view(st *styles, input string) string {
	lines := []string{st.boxTitle.Render(p.title), input}
	start := max(0, min(p.cursor-pickerRows/2, len(p.shown)-pickerRows))
	for i := start; i < min(start+pickerRows, len(p.shown)); i++ {
		c := p.shown[i]
		label := fmt.Sprintf("%-32s %5d", c.name, c.jobs)
		if c.name == "" {
			label = fmt.Sprintf("%-32s", "(all "+p.title+"s)")
		}
		if i == p.cursor {
			label = st.selected.Render(label)
		}
		lines = append(lines, label)
	}
	if len(p.shown) == 1 {
		lines = append(lines, st.dim.Render("no "+p.title+" matches"))
	}
	return st.box.Render(strings.Join(lines, "\n"))
}

// namespaces are those of the jobs read, before the namespace filter, with their numbers of jobs.
func (m *Model) namespaces() []choice { return m.nsChoices }

// contexts are those of the scopes, with their numbers of jobs.
func (m *Model) contexts() []choice { return m.ctxChoices }

func (m *Model) count(by func(ctx, ns string) string) []choice {
	n := map[string]int{}
	for i := range m.scopes {
		for j := range m.scopes[i].Jobs {
			job := &m.scopes[i].Jobs[j]
			n[by(job.Context, job.Namespace)]++
		}
	}
	out := make([]choice, 0, len(n))
	for name, jobs := range n {
		out = append(out, choice{name, jobs})
	}
	slices.SortFunc(out, func(a, b choice) int { return cmp.Compare(a.name, b.name) })
	return out
}

// helpView lists the keys in columns side by side, and the commands, so it fits a 80×24 terminal.
func (m *Model) helpView() string {
	groups := m.keys.full()
	cols := make([]string, 0, 2*len(groups))
	for _, group := range groups {
		var lines []string
		for _, b := range group {
			lines = append(lines, fmt.Sprintf("%-12s %s", b.Help().Key, b.Help().Desc))
		}
		cols = append(cols, strings.Join(lines, "\n"), "   ")
	}
	commands := strings.Join([]string{
		":ns NAME|all    :ctx NAME|all    :q",
		":sort " + strings.Join(fleet.SortKeys, "|"),
		":filter all|unhealthy|FAILED|STUCK|…",
	}, "\n")
	keys := lipgloss.JoinHorizontal(lipgloss.Top, cols[:len(cols)-1]...)
	if lipgloss.Width(keys)+4 > m.width { // border and padding
		keys = strings.Join(slices.DeleteFunc(cols, func(c string) bool { return c == "   " }), "\n\n")
	}
	return m.st.box.Render(m.st.boxTitle.Render("keys") + "\n" + keys + "\n\n" + commands)
}
