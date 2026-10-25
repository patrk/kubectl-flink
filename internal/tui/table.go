package tui

import (
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

// table is a virtual table in the manner of tview's: the content is set when the data changes, with column
// widths computed once over every row; a frame renders only the rows in view. The cursor follows a row's key, so
// a re-sort under the cursor keeps the same job selected.
type table struct {
	header []string
	rows   [][]string
	keys   []string
	styles []lipgloss.Style // per row, for the health cell
	widths []int
	health int // index of the health column, -1 if none

	cursor, offset int
}

const gap = 3 // spaces between columns, as ls prints them

// setContent replaces the rows and keeps the cursor on the row with the same key, or at the same index when that
// row is gone.
func (t *table) setContent(header []string, rows [][]string, keys []string, styles []lipgloss.Style, health int) {
	selected := t.selectedKey()
	t.header, t.rows, t.keys, t.styles, t.health = header, rows, keys, styles, health
	t.widths = make([]int, len(header))
	for c, h := range header {
		t.widths[c] = ansi.StringWidth(h)
	}
	for _, r := range rows {
		for c, cell := range r {
			if w := ansi.StringWidth(cell); c < len(t.widths) && w > t.widths[c] {
				t.widths[c] = w
			}
		}
	}
	if selected != "" {
		for i, k := range keys {
			if k == selected {
				t.cursor = i
				return
			}
		}
	}
	t.cursor = min(t.cursor, max(len(rows)-1, 0))
}

func (t *table) selectedKey() string {
	if t.cursor < len(t.keys) {
		return t.keys[t.cursor]
	}
	return ""
}

// move moves the cursor by delta rows, within the table.
func (t *table) move(delta int) {
	t.cursor = max(0, min(t.cursor+delta, len(t.rows)-1))
}

// scroll keeps the cursor within the height rows in view.
func (t *table) scroll(height int) {
	if t.cursor < t.offset {
		t.offset = t.cursor
	}
	if t.cursor >= t.offset+height {
		t.offset = t.cursor - height + 1
	}
	t.offset = max(0, min(t.offset, len(t.rows)-height))
}

// view renders the header and the height rows in view, each cut to width; the last column takes what is left.
func (t *table) view(width, height int, st *styles) string {
	t.scroll(height)
	var b strings.Builder
	b.WriteString(st.header.Render(t.line(t.header, width)))
	end := min(t.offset+height, len(t.rows))
	for i := t.offset; i < end; i++ {
		b.WriteByte('\n')
		if i == t.cursor {
			b.WriteString(st.selected.Render(t.line(t.rows[i], width)))
			continue
		}
		b.WriteString(t.styledLine(i, width))
	}
	return b.String()
}

func (t *table) line(cells []string, width int) string {
	var b strings.Builder
	for c, cell := range cells {
		if c > 0 {
			b.WriteString(strings.Repeat(" ", gap))
		}
		b.WriteString(cell)
		if c < len(cells)-1 {
			b.WriteString(strings.Repeat(" ", t.widths[c]-ansi.StringWidth(cell)))
		}
	}
	return ansi.Truncate(b.String(), width, "…")
}

// styledLine is a row with its health cell coloured; the cut is made on the plain line first, so the colour
// cannot change where it falls.
func (t *table) styledLine(i, width int) string {
	plain := t.line(t.rows[i], width)
	if t.health < 0 {
		return plain
	}
	start := 0
	for c := range t.health {
		start += t.widths[c] + gap
	}
	cell := t.rows[i][t.health]
	if start+len(cell) > len(plain) || plain[start:start+len(cell)] != cell {
		return plain // cut inside the health cell, or a wide rune before it
	}
	return plain[:start] + t.styles[i].Render(cell) + plain[start+len(cell):]
}
