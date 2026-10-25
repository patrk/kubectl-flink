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
	magenta = lipgloss.Color("5")
	cyan    = lipgloss.Color("6")
)

const errorStyle = render.ErrorHealth

type styles struct {
	title, header, selected, dim, warn, flash, box, boxTitle lipgloss.Style
	// health colours a health cell by its text, ERROR included.
	health map[string]lipgloss.Style
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
	}
}
