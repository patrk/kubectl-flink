package tui

import (
	"image/color"

	"charm.land/lipgloss/v2"

	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
)

// The 16 ANSI colours, so the terminal's theme decides the shades, as ls's colours do.
var (
	red     = lipgloss.Color("1")
	green   = lipgloss.Color("2")
	yellow  = lipgloss.Color("3")
	blue    = lipgloss.Color("4")
	magenta = lipgloss.Color("5")
	cyan    = lipgloss.Color("6")
)

const errorStyle = render.ErrorHealth

type styles struct {
	title, header, selected, dim, warn, flash, box, boxTitle lipgloss.Style
	// health colours a health cell by its text, ERROR included.
	health map[string]lipgloss.Style
	// state colours an operator by what fills its time, as the Flink web UI does; "" is no metrics.
	state          map[string]lipgloss.Style
	tab, activeTab lipgloss.Style
}

func newStyles() styles {
	fg := func(c color.Color) lipgloss.Style { return lipgloss.NewStyle().Foreground(c) }
	return styles{
		title:    lipgloss.NewStyle().Bold(true),
		header:   lipgloss.NewStyle().Bold(true),
		selected: lipgloss.NewStyle().Reverse(true),
		dim:      lipgloss.NewStyle().Faint(true),
		warn:     fg(yellow),
		flash:    fg(cyan),
		box:      lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1),
		boxTitle: lipgloss.NewStyle().Bold(true),
		health: map[string]lipgloss.Style{
			model.OK.String():          fg(green),
			model.Suspended.String():   lipgloss.NewStyle().Faint(true),
			model.Progressing.String(): fg(cyan),
			model.Degraded.String():    fg(yellow),
			model.Stuck.String():       fg(magenta),
			model.Failed.String():      fg(red),
			errorStyle:                 fg(red),
		},
		state: map[string]lipgloss.Style{
			render.Busy:    fg(red),
			render.Pressed: fg(lipgloss.Color("8")), // the UI's black, visible on a dark terminal
			render.Idle:    fg(blue),
			"":             lipgloss.NewStyle(),
		},
		tab:       lipgloss.NewStyle().Padding(0, 1),
		activeTab: lipgloss.NewStyle().Padding(0, 1).Reverse(true).Bold(true),
	}
}
