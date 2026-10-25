// Package tui is kubectl flink top: the fleet as a live table, over the same rows, filters and sort as ls.
package tui

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
)

// ReassessEvery re-derives health without events, because a job turns DEGRADED or STUCK by time alone.
const ReassessEvery = 5 * time.Second

// reasonWidth only bounds the reason; the table cuts it to the terminal.
const reasonWidth = 300

// Config is what top shows and how it starts.
type Config struct {
	// Updates delivers the scopes after every change; top quits when it closes.
	Updates <-chan []fleet.Scope
	Now     func() time.Time
	// Thresholds and IDLabels come from the profile.
	Thresholds model.Thresholds
	IDLabels   []string
	// Search, Health and Sort are the starting search text, health filter and sort key.
	Search string
	Health []model.Health
	Sort   string
	// Loader collects a job's detail for Enter; without one, Enter shows the why command.
	Loader Loader
	// Context ends the collections when top ends; nil is context.Background.
	Context context.Context
}

type mode int

const (
	modeList mode = iota
	modeSearch
	modeCommand
	modeHelp
	modePicker
	modeDetail
)

// Model is the top screen. It is driven by messages only: scopes from Config.Updates, ticks and keys.
type Model struct {
	cfg    Config
	st     styles
	keys   keyMap
	mode   mode
	width  int
	height int

	scopes []fleet.Scope
	loaded bool

	filter    int // index into filters
	custom    []model.Health
	sortIdx   int
	search    string
	namespace string
	context   string

	// assessed is every row at the last data or tick; filters and search re-apply to it without re-assessing.
	assessed   []fleet.Row
	nsChoices  []choice
	ctxChoices []choice
	shown      []fleet.Row
	counts     string
	// err ends top: the only scope could not be read.
	err error

	table table
	input textinput.Model

	ctx     context.Context
	gen     int // of the last cursor move, so that a prefetch fires only where the cursor rests
	loading inflight
	cache   map[string]*entry
	detail  detailState
	// reporting are the jobs whose report is being written, by key.
	reporting map[string]bool
	// wide adds ls -o wide's columns.
	wide bool
	// quitting is set by a quit that a report in progress held back.
	quitting bool

	picker picker
	flash  string
}

type scopesMsg []fleet.Scope

type tickMsg time.Time

type closedMsg struct{}

type healthFilter struct {
	name   string
	health []model.Health
}

// filters are the steps of f: everything, then the unhealthy, then one health at a time from the worst.
var filters = []healthFilter{
	{"all", nil},
	{"unhealthy", fleet.UnhealthyValues()},
	{"FAILED", []model.Health{model.Failed}},
	{"STUCK", []model.Health{model.Stuck}},
	{"DEGRADED", []model.Health{model.Degraded}},
	{"PROGRESSING", []model.Health{model.Progressing}},
	{"SUSPENDED", []model.Health{model.Suspended}},
	{"OK", []model.Health{model.OK}},
}

// New returns the model; the program starts it with Init.
func New(cfg Config) *Model {
	m := &Model{cfg: cfg, st: newStyles(), keys: newKeyMap(), search: cfg.Search, width: 80, height: 24,
		ctx: cfg.Context, cache: map[string]*entry{}, reporting: map[string]bool{}}
	if m.ctx == nil {
		m.ctx = context.Background()
	}
	// --unhealthy or one --health value is a step of f, so f goes on from there.
	if i := slices.IndexFunc(filters, func(f healthFilter) bool { return sameHealth(f.health, cfg.Health) }); i >= 0 {
		m.filter = i
	} else {
		m.custom = cfg.Health
	}
	if i := slices.Index(fleet.SortKeys, cfg.Sort); i >= 0 {
		m.sortIdx = i
	}
	m.input = textinput.New()
	m.input.SetVirtualCursor(true)
	return m
}

// Init waits for the first scopes and starts the reassessment tick.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.wait(), m.tick())
}

func (m *Model) wait() tea.Cmd {
	ch := m.cfg.Updates
	return func() tea.Msg {
		s, ok := <-ch
		if !ok {
			return closedMsg{}
		}
		return scopesMsg(s)
	}
}

func (m *Model) tick() tea.Cmd {
	return tea.Tick(ReassessEvery, func(t time.Time) tea.Msg { return tickMsg(t) })
}

// Update handles one message.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		if m.mode == modeDetail {
			m.fillDetail(false)
		}
		return m, nil
	case prefetchMsg:
		cmd := m.prefetch(msg)
		return m, cmd
	case detailMsg:
		m.collected(msg)
		cmd := m.tabOpened()
		return m, cmd
	case logMsg:
		m.logRead(msg)
		return m, nil
	case reportMsg:
		m.reported(msg)
		return m, nil
	case scopesMsg:
		if !m.loaded && len(msg) == 1 && msg[0].Err != nil {
			m.err = msg[0].Err // nothing to show; ls says so the same way
			return m, tea.Quit
		}
		first := !m.loaded
		m.scopes, m.loaded = msg, true
		m.reassess()
		cmd := m.wait()
		if first {
			cmd = tea.Batch(cmd, m.moved())
		}
		return m, cmd
	case tickMsg:
		m.reassess()
		cmd := m.tick()
		return m, cmd
	case closedMsg:
		return m, tea.Quit
	case tea.KeyPressMsg:
		cmd := m.key(msg)
		return m, cmd
	}
	return m, nil
}

func (m *Model) key(msg tea.KeyPressMsg) tea.Cmd {
	if key.Matches(msg, m.keys.forceQuit) {
		return m.quit()
	}
	switch m.mode {
	case modeSearch:
		return m.searchKey(msg)
	case modeCommand:
		return m.commandKey(msg)
	case modeHelp:
		if key.Matches(msg, m.keys.back, m.keys.help, m.keys.quit) {
			m.mode = modeList
		}
		return nil
	case modePicker:
		return m.pickerKey(msg)
	case modeDetail:
		return m.detailKey(msg)
	}
	m.flash = ""
	switch {
	case key.Matches(msg, m.keys.quit):
		return m.quit()
	case key.Matches(msg, m.keys.up):
		m.table.move(-1)
		return m.moved()
	case key.Matches(msg, m.keys.down):
		m.table.move(1)
		return m.moved()
	case key.Matches(msg, m.keys.pageUp):
		m.table.move(-m.bodyHeight())
		return m.moved()
	case key.Matches(msg, m.keys.pageDown):
		m.table.move(m.bodyHeight())
		return m.moved()
	case key.Matches(msg, m.keys.top):
		m.table.cursor = 0
		return m.moved()
	case key.Matches(msg, m.keys.bottom):
		m.table.move(len(m.table.rows))
		return m.moved()
	case key.Matches(msg, m.keys.search):
		m.mode = modeSearch
		return m.prompt("/", m.search, nil)
	case key.Matches(msg, m.keys.command):
		m.mode = modeCommand
		return m.prompt(":", "", m.commandSuggestions())
	case key.Matches(msg, m.keys.filter):
		m.custom = nil
		m.filter = (m.filter + 1) % len(filters)
		m.apply()
	case key.Matches(msg, m.keys.sort):
		m.sortIdx = (m.sortIdx + 1) % len(fleet.SortKeys)
		m.apply()
	case key.Matches(msg, m.keys.namespace):
		m.openPicker("namespace", m.namespaces(), m.namespace)
	case key.Matches(msg, m.keys.context):
		if len(m.scopes) < 2 {
			m.flash = "only one context is shown; start top with --context or a profile that lists several"
			return nil
		}
		m.openPicker("context", m.contexts(), m.context)
	case key.Matches(msg, m.keys.enter):
		return m.openDetail()
	case key.Matches(msg, m.keys.report):
		if r := m.selected(); r != nil {
			return m.report(*r.Job)
		}
	case key.Matches(msg, m.keys.wide):
		m.wide = !m.wide
		m.apply()
	case key.Matches(msg, m.keys.help):
		m.mode = modeHelp
	case key.Matches(msg, m.keys.back):
		m.search, m.namespace, m.context, m.custom, m.filter = "", "", "", nil, 0
		m.apply()
	}
	return nil
}

// prompt focuses the input line with a prompt, a starting value and completions.
func (m *Model) prompt(p, value string, suggestions []string) tea.Cmd {
	m.input.Prompt = p
	m.input.SetValue(value)
	m.input.CursorEnd()
	m.input.SetSuggestions(suggestions)
	m.input.ShowSuggestions = len(suggestions) > 0
	return m.input.Focus()
}

// searchKey filters as the user types; Enter keeps the search, Esc drops it.
func (m *Model) searchKey(msg tea.KeyPressMsg) tea.Cmd {
	switch {
	case key.Matches(msg, m.keys.enter):
		m.mode = modeList
		m.input.Blur()
		return nil
	case key.Matches(msg, m.keys.back):
		m.mode, m.search = modeList, ""
		m.input.Blur()
		m.apply()
		return nil
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	if v := strings.TrimSpace(m.input.Value()); v != m.search {
		m.search = v
		m.apply()
	}
	return cmd
}

func (m *Model) commandKey(msg tea.KeyPressMsg) tea.Cmd {
	switch {
	case key.Matches(msg, m.keys.enter):
		m.mode = modeList
		m.input.Blur()
		return m.run(m.input.Value())
	case key.Matches(msg, m.keys.back):
		m.mode = modeList
		m.input.Blur()
		return nil
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return cmd
}

func (m *Model) commandSuggestions() []string {
	nss, ctxs := m.namespaces(), m.contexts()
	out := make([]string, 0, 5+len(nss)+len(ctxs)+len(fleet.SortKeys))
	out = append(out, "quit", "ns all", "ctx all", "filter all", "filter unhealthy")
	for _, ns := range nss {
		out = append(out, "ns "+ns.name)
	}
	for _, c := range ctxs {
		out = append(out, "ctx "+c.name)
	}
	for _, k := range fleet.SortKeys {
		out = append(out, "sort "+k)
	}
	return out
}

// run executes a : command.
func (m *Model) run(line string) tea.Cmd {
	verb, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
	arg = strings.TrimSpace(arg)
	if arg == "all" {
		arg = ""
	}
	switch verb {
	case "":
		return nil
	case "q", "quit":
		return m.quit()
	case "ns", "namespace":
		m.namespace = arg
	case "ctx", "context":
		m.context = arg
	case "sort":
		i := slices.Index(fleet.SortKeys, arg)
		if i < 0 {
			m.flash = fmt.Sprintf("unknown sort key %q, use one of %s", arg, strings.Join(fleet.SortKeys, ", "))
			return nil
		}
		m.sortIdx = i
	case "filter":
		i := slices.IndexFunc(filters, func(f healthFilter) bool { return strings.EqualFold(f.name, cmp.Or(arg, "all")) })
		if i < 0 {
			m.flash = fmt.Sprintf("unknown filter %q, use all, unhealthy or a health value", arg)
			return nil
		}
		m.filter, m.custom = i, nil
	default:
		m.flash = fmt.Sprintf("unknown command %q; ? lists the keys and commands", verb)
		return nil
	}
	m.apply()
	return nil
}

func (m *Model) health() []model.Health {
	if m.custom != nil {
		return m.custom
	}
	return filters[m.filter].health
}

func (m *Model) filterName() string {
	if m.custom != nil {
		names := make([]string, len(m.custom))
		for i, h := range m.custom {
			names[i] = h.String()
		}
		return strings.Join(names, ",")
	}
	return filters[m.filter].name
}

// reassess derives every row's health at now, on new data and on the tick, and re-applies the filters.
func (m *Model) reassess() {
	m.assessed = fleet.Rows(m.scopes, m.cfg.Now(), m.cfg.Thresholds)
	m.nsChoices = m.count(func(_, ns string) string { return ns })
	m.ctxChoices = m.count(func(ctx, _ string) string { return ctx })
	for i := range m.scopes {
		if !slices.ContainsFunc(m.ctxChoices, func(c choice) bool { return c.name == m.scopes[i].Context }) {
			m.ctxChoices = append(m.ctxChoices, choice{name: m.scopes[i].Context})
		}
	}
	slices.SortFunc(m.ctxChoices, func(a, b choice) int { return cmp.Compare(a.name, b.name) })
	m.apply()
}

// apply filters and sorts the assessed rows and rebuilds the table content. It runs on data, filter, search and
// sort changes, never per frame.
func (m *Model) apply() {
	now := m.cfg.Now()
	f := fleet.Filter{Health: m.health(), Text: m.search, IDLabels: m.cfg.IDLabels}
	rows := f.Apply(m.assessed)
	rows = slices.DeleteFunc(rows, func(r fleet.Row) bool {
		return (m.namespace != "" && r.Job.Namespace != m.namespace) || (m.context != "" && r.Job.Context != m.context)
	})
	_ = fleet.Sort(rows, fleet.SortKeys[m.sortIdx]) // the key comes from SortKeys
	m.shown = rows
	s := render.Summarise(rows)
	m.counts, _, _ = strings.Cut(s.Line(), " · ") // the pods and requests do not fit the title

	o := render.ScopeOptions(m.scopes)
	o.ShowNamespace = o.ShowNamespace || m.namespace == "" && len(m.nsChoices) > 1
	o.ReasonWidth, o.Wide, o.NoSnapshotAge = reasonWidth, m.wide, true
	header := o.Header()
	health := slices.Index(header, render.HealthColumn)
	var cells [][]string
	var keys []string
	var rowStyles []lipgloss.Style
	for i := range m.scopes {
		if s := &m.scopes[i]; s.Err != nil && (m.context == "" || m.context == s.Context) {
			cells, keys, rowStyles = append(cells, o.ErrorCells(s)), append(keys, "scope\x00"+s.Context), append(rowStyles, m.st.health[errorStyle])
		}
	}
	for i := range rows {
		cells = append(cells, o.Cells(&rows[i], now))
		keys = append(keys, render.Key(rows[i].Job))
		rowStyles = append(rowStyles, m.st.health[rows[i].Health.String()])
	}
	m.table.setContent(header, cells, keys, rowStyles, health)
}

func (m *Model) selected() *fleet.Row {
	k := m.table.selectedKey()
	for i := range m.shown {
		if render.Key(m.shown[i].Job) == k {
			return &m.shown[i]
		}
	}
	return nil
}

// quit ends top, unless a report is still being written: then the first quit says so and the second drops it.
func (m *Model) quit() tea.Cmd {
	if len(m.reporting) > 0 && !m.quitting {
		m.quitting = true
		m.flash = "a report is still being written; quit again to drop it"
		return nil
	}
	return tea.Quit
}

func sameHealth(a, b []model.Health) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func jobRef(j *model.Job) string {
	ref := j.Namespace + "/" + j.Name
	if j.Context != "" {
		ref = "--context " + j.Context + " " + ref
	}
	return ref
}

// bodyHeight is the number of table rows on screen: the screen less the title, status, header and footer lines.
func (m *Model) bodyHeight() int { return max(1, m.height-4) }

// View renders the screen.
func (m *Model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = true
	return v
}

func (m *Model) render() string {
	if m.mode == modeDetail {
		return m.detailView()
	}
	if !m.loaded {
		return m.st.title.Render(cut("kubectl flink top · loading Flink jobs…", m.width))
	}
	var body string
	switch m.mode {
	case modeHelp:
		body = lipgloss.Place(m.width, m.bodyHeight()+1, lipgloss.Center, lipgloss.Center, m.helpView())
	case modePicker:
		body = lipgloss.Place(m.width, m.bodyHeight()+1, lipgloss.Center, lipgloss.Center, m.picker.view(&m.st, m.input.View()))
	default:
		body = m.table.view(m.width, m.bodyHeight(), &m.st)
		if len(m.table.rows) == 0 {
			body += "\n" + m.st.dim.Render(cut("  no jobs match; Esc clears search and filters", m.width))
		}
	}
	// The body fills the lines between the status line and the footer, cut or padded to them.
	rows := strings.Split(body, "\n")
	rows = rows[:min(len(rows), m.bodyHeight()+1)]
	for i := range rows {
		rows[i] = cut(rows[i], m.width)
	}
	for len(rows) < m.bodyHeight()+1 {
		rows = append(rows, "")
	}
	return strings.Join(append(append([]string{m.titleLine(), m.statusLine()}, rows...), m.footer()), "\n")
}

func (m *Model) titleLine() string {
	ctxs := make([]string, 0, len(m.scopes))
	for i := range m.scopes {
		ctxs = append(ctxs, m.scopes[i].Context)
	}
	return m.st.title.Render(cut("kubectl flink top · "+strings.Join(ctxs, ", ")+" · "+m.counts, m.width))
}

// statusLine shows the filter, sort and search in force, then the first problem with the data: a failing watch,
// then the notes on how scopes were read.
func (m *Model) statusLine() string {
	parts := []string{"filter: " + m.filterName(), "sort: " + fleet.SortKeys[m.sortIdx]}
	if m.search != "" {
		parts = append(parts, "search: "+m.search)
	}
	if m.namespace != "" {
		parts = append(parts, "namespace: "+m.namespace)
	}
	if m.context != "" {
		parts = append(parts, "context: "+m.context)
	}
	line := m.st.dim.Render(strings.Join(parts, "   "))
	var warn []string
	for i := range m.scopes {
		if s := &m.scopes[i]; s.WatchErr != nil {
			warn = append(warn, fmt.Sprintf("context %s: %s; rows may be stale", s.Context, s.WatchErr))
		}
	}
	warn = append(warn, render.Notes(m.scopes)...)
	if len(warn) == 0 {
		return cut(line, m.width)
	}
	if len(warn) > 1 {
		warn[0] += fmt.Sprintf(" (+%d more)", len(warn)-1)
	}
	// The warning first: when the line is too long, the filters are what is cut.
	return cut(m.st.warn.Render(warn[0])+"   "+line, m.width)
}

func (m *Model) footer() string {
	switch {
	case m.mode == modeSearch || m.mode == modeCommand:
		return cut(m.input.View(), m.width)
	case m.flash != "":
		return m.st.flash.Render(cut(m.flash, m.width))
	}
	return m.st.dim.Render(cut(m.keys.short(), m.width))
}

func cut(s string, width int) string { return ansi.Truncate(s, width, "…") }
