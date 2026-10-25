package tui

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/patrk/kubectl-flink/internal/history"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// Detail is what top shows of one job: why's evidence and verdict, collected once, and the history built from
// them. Each tab draws from it at the pane's width.
type Detail struct {
	Evidence *triage.Evidence
	Verdict  *triage.Verdict
	// History is nil when it could not be built; HistoryErr says why.
	History    *history.History
	HistoryErr error
	Loaded     time.Time
}

// Loader collects a job's detail and reads its JobManager log. top's command implements it with the session's
// clients and port-forwards.
type Loader interface {
	Detail(ctx context.Context, job model.Job) (*Detail, error)
	Log(ctx context.Context, job model.Job, tail int) (string, error)
}

// Timing of the detail data.
const (
	// prefetchAfter is how long the cursor rests on a row before its detail is collected.
	prefetchAfter = 150 * time.Millisecond
	// fresh is how long a collected detail is shown without collecting it again.
	fresh = 30 * time.Second
	// cacheSize bounds the details kept, the least recently opened dropped first.
	cacheSize = 16
	// logTail is how many lines of the JobManager log the Logs tab shows.
	logTail = 300
	// loadTimeout bounds one collection; why's own collection bounds itself a little earlier.
	loadTimeout = triage.Timeout + 5*time.Second
)

// tabs of the detail screen, each with the command that prints the same.
var tabs = []struct{ name, command string }{
	{"Why", "why"}, {"Graph", "graph"}, {"Exceptions", "exceptions"}, {"Checkpoints", "checkpoints"},
	{"Snapshots", "snapshots"}, {"Logs", "logs"}, {"History", "history"},
}

// entry is a job's cached detail and log.
type entry struct {
	detail      *Detail
	err         error
	log         string
	logErr      error
	logLoaded   bool
	logLoading  bool
	lastOpened  time.Time
	collectedAt time.Time
}

// detailState is the open detail screen.
type detailState struct {
	key  string
	job  model.Job
	tab  int
	view viewport.Model
}

// inflight is the one collection running; a new one cancels it.
type inflight struct {
	key    string
	cancel context.CancelFunc
}

type prefetchMsg struct{ gen int }

type detailMsg struct {
	key    string
	detail *Detail
	err    error
}

type logMsg struct {
	key string
	log string
	err error
}

// moved schedules a prefetch of the selected row's detail, which runs only if the cursor is still there after
// prefetchAfter: scrolling through rows collects nothing.
func (m *Model) moved() tea.Cmd {
	if m.cfg.Loader == nil {
		return nil
	}
	m.gen++
	gen := m.gen
	return tea.Tick(prefetchAfter, func(time.Time) tea.Msg { return prefetchMsg{gen} })
}

func (m *Model) prefetch(msg prefetchMsg) tea.Cmd {
	if msg.gen != m.gen || m.mode != modeList {
		return nil
	}
	r := m.selected()
	if r == nil {
		return nil
	}
	if e := m.cache[render.Key(r.Job)]; e != nil && e.err == nil && m.cfg.Now().Sub(e.collectedAt) < fresh {
		return nil
	}
	return m.collect(*r.Job)
}

// collect starts collecting job's detail, cancelling the collection in flight: one request set at a time.
func (m *Model) collect(job model.Job) tea.Cmd {
	k := render.Key(&job)
	if m.loading.key == k {
		return nil
	}
	if m.loading.cancel != nil {
		m.loading.cancel()
	}
	ctx, cancel := context.WithTimeout(m.ctx, loadTimeout)
	m.loading = inflight{key: k, cancel: cancel}
	loader := m.cfg.Loader
	return func() tea.Msg {
		defer cancel()
		d, err := loader.Detail(ctx, job)
		return detailMsg{key: k, detail: d, err: err}
	}
}

func (m *Model) collected(msg detailMsg) {
	if m.loading.key == msg.key {
		m.loading = inflight{}
	}
	if errors.Is(msg.err, context.Canceled) {
		return // the cursor moved on
	}
	e := m.entry(msg.key)
	e.detail, e.err, e.collectedAt = msg.detail, msg.err, m.cfg.Now()
	e.log, e.logErr, e.logLoaded = "", nil, false
	if m.mode == modeDetail && m.detail.key == msg.key {
		m.fillDetail(true)
	}
}

// entry is the cache entry of key, created when missing; the cache drops the least recently opened beyond its size.
func (m *Model) entry(k string) *entry {
	if e := m.cache[k]; e != nil {
		return e
	}
	if len(m.cache) >= cacheSize {
		oldest := ""
		for ck, e := range m.cache {
			if ck != m.detail.key && (oldest == "" || e.lastOpened.Before(m.cache[oldest].lastOpened)) {
				oldest = ck
			}
		}
		delete(m.cache, oldest)
	}
	e := &entry{}
	m.cache[k] = e
	return e
}

// openDetail shows the selected job's detail, collecting it when it is not cached.
func (m *Model) openDetail() tea.Cmd {
	r := m.selected()
	if r == nil {
		return nil
	}
	if m.cfg.Loader == nil {
		m.flash = "kubectl flink why " + jobRef(r.Job)
		return nil
	}
	k := render.Key(r.Job)
	m.mode = modeDetail
	m.detail.key, m.detail.job = k, *r.Job
	m.detail.view = viewport.New(viewport.WithWidth(m.width), viewport.WithHeight(m.detailHeight()))
	e := m.entry(k)
	e.lastOpened = m.cfg.Now()
	m.fillDetail(true)
	// What is cached shows at once; a failed or old collection is made again behind it.
	if e.detail == nil || e.err != nil || m.cfg.Now().Sub(e.collectedAt) >= fresh {
		return tea.Batch(m.collect(*r.Job), m.tabOpened())
	}
	return m.tabOpened()
}

// current is the open job as the watch last saw it, so that collecting again uses its status now: a JobManager
// that has become ready, a new job ID after an upgrade. A job gone from the watch keeps its last state.
func (m *Model) current() model.Job {
	for i := range m.assessed {
		if render.Key(m.assessed[i].Job) == m.detail.key {
			return *m.assessed[i].Job
		}
	}
	return m.detail.job
}

// tabOpened reads the log when the Logs tab is first opened: it is not part of the collection.
func (m *Model) tabOpened() tea.Cmd {
	e := m.cache[m.detail.key]
	if m.mode != modeDetail || tabs[m.detail.tab].name != "Logs" || e == nil || e.logLoaded || e.logLoading {
		return nil
	}
	e.logLoading = true
	k, job, loader := m.detail.key, m.detail.job, m.cfg.Loader
	ctx := m.ctx
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(ctx, loadTimeout)
		defer cancel()
		log, err := loader.Log(ctx, job, logTail)
		return logMsg{key: k, log: log, err: err}
	}
}

func (m *Model) logRead(msg logMsg) {
	e := m.cache[msg.key]
	if e == nil {
		return
	}
	e.log, e.logErr, e.logLoaded, e.logLoading = msg.log, msg.err, true, false
	if m.mode == modeDetail && m.detail.key == msg.key && tabs[m.detail.tab].name == "Logs" {
		m.fillDetail(false)
		m.detail.view.GotoBottom() // the newest lines
	}
}

func (m *Model) detailKey(msg tea.KeyPressMsg) tea.Cmd {
	m.flash = ""
	switch s := msg.String(); {
	case key.Matches(msg, m.keys.back):
		m.mode = modeList
		return nil
	case key.Matches(msg, m.keys.quit):
		return tea.Quit
	case s == "tab" || s == "]":
		return m.setTab((m.detail.tab + 1) % len(tabs))
	case s == "shift+tab" || s == "[":
		return m.setTab((m.detail.tab + len(tabs) - 1) % len(tabs))
	case len(s) == 1 && s >= "1" && s <= strconv.Itoa(len(tabs)):
		return m.setTab(int(s[0] - '1'))
	case s == "r":
		e := m.entry(m.detail.key)
		e.detail, e.err = nil, nil
		m.fillDetail(true)
		return m.collect(m.current())
	case s == "y":
		c := m.tabCommand()
		m.flash = "copied: " + c
		return tea.SetClipboard(c)
	}
	var cmd tea.Cmd
	m.detail.view, cmd = m.detail.view.Update(msg)
	return cmd
}

func (m *Model) setTab(i int) tea.Cmd {
	m.detail.tab = i
	m.fillDetail(true)
	return m.tabOpened()
}

// tabCommand is the command that prints the open tab.
func (m *Model) tabCommand() string {
	return "kubectl flink " + tabs[m.detail.tab].command + " " + jobRef(&m.detail.job)
}

func (m *Model) detailHeight() int { return max(1, m.height-3) }

// fillDetail draws the open tab into the viewport, from the top when top is set.
func (m *Model) fillDetail(top bool) {
	m.detail.view.SetWidth(m.width)
	m.detail.view.SetHeight(m.detailHeight())
	m.detail.view.SetContent(m.tabContent())
	if top {
		m.detail.view.GotoTop()
	}
}

func (m *Model) tabContent() string {
	e := m.cache[m.detail.key]
	switch {
	case e == nil || e.detail == nil && e.err == nil:
		return m.st.dim.Render("collecting " + m.detail.job.Namespace + "/" + m.detail.job.Name + "…")
	case e.err != nil:
		return m.st.warn.Render("could not collect the job: " + e.err.Error())
	}
	d, now := e.detail, m.cfg.Now()
	ev := d.Evidence
	var b bytes.Buffer
	var err error
	switch tabs[m.detail.tab].name {
	case "Why":
		err = render.WhyWidth(&b, ev, d.Verdict, m.width)
	case "Graph":
		return graphView(render.NewJobGraph(ev), m.width, &m.st)
	case "Exceptions":
		if ev.REST == nil || ev.REST.Job == nil {
			return notCollected(ev, "the exception history")
		}
		rep := render.NewExceptionReport(ev.Job, ev.REST.Exceptions, ev.REST.ExceptionsTruncated)
		err = render.Exceptions(&b, &rep, false, now)
	case "Checkpoints":
		if ev.REST == nil || ev.REST.Job == nil || ev.REST.Checkpoints == nil {
			return notCollected(ev, "the checkpoints")
		}
		rep := render.NewCheckpointReport(ev.Job, ev.REST.Checkpoints, ev.REST.CheckpointConfig, now, 10)
		err = render.Checkpoints(&b, &rep, now)
	case "Snapshots":
		rep := render.SnapshotReport{Job: render.NewJobRef(ev.Job), Snapshots: []render.SnapshotItem{}}
		for i := range ev.Snapshots {
			rep.Snapshots = append(rep.Snapshots, render.NewSnapshotItem(&ev.Snapshots[i]))
		}
		if ev.REST != nil {
			rep.JobManager = ev.REST.Checkpoints
		}
		for _, g := range ev.Gaps {
			if g.What == "FlinkStateSnapshots" || g.What == "JobManager REST API" {
				rep.NotCollected = append(rep.NotCollected, g.What+": "+g.Why)
			}
		}
		err = render.Snapshots(&b, &rep, now)
	case "Logs":
		switch {
		case !e.logLoaded:
			return m.st.dim.Render("reading the JobManager log…")
		case e.logErr != nil:
			return m.st.warn.Render("could not read the JobManager log: " + e.logErr.Error())
		}
		return e.log
	case "History":
		if d.History == nil {
			return m.st.warn.Render(fmt.Sprintf("no history: %v", d.HistoryErr))
		}
		err = render.HistoryWidth(&b, render.NewJobRef(ev.Job), d.History, now, m.width)
	}
	if err != nil {
		return m.st.warn.Render(err.Error())
	}
	return strings.TrimRight(b.String(), "\n")
}

// notCollected says why the JobManager's part of a tab is missing.
func notCollected(ev *triage.Evidence, what string) string {
	for _, g := range ev.Gaps {
		if g.What == "JobManager REST API" {
			return "No " + strings.TrimPrefix(what, "the ") + ": " + g.Why + "."
		}
	}
	return "No " + strings.TrimPrefix(what, "the ") + ": the JobManager was not asked."
}

// detailView is the detail screen: the job and when it was collected, the tabs, the open tab and the footer.
func (m *Model) detailView() string {
	j := &m.detail.job
	title := "kubectl flink top · " + j.Context + " · " + j.Namespace + "/" + j.Name
	if e := m.cache[m.detail.key]; e != nil && e.detail != nil {
		title += " · " + e.detail.Evidence.Health.Health.String() + " · collected " +
			duration.HumanDuration(m.cfg.Now().Sub(e.collectedAt)) + " ago"
	}
	if m.loading.key == m.detail.key {
		title += " · collecting…"
	}
	bar := make([]string, len(tabs))
	for i, t := range tabs {
		label := strconv.Itoa(i+1) + " " + t.name
		if i == m.detail.tab {
			bar[i] = m.st.activeTab.Render(label)
		} else {
			bar[i] = m.st.tab.Render(label)
		}
	}
	footer := m.st.dim.Render("esc back  1-7 tab  ↑↓ pgup pgdn scroll  ←→ pan  r collect again  y copy: " + m.tabCommand())
	if m.flash != "" {
		footer = m.st.flash.Render(m.flash)
	}
	return strings.Join([]string{
		m.st.title.Render(cut(title, m.width)),
		cut(strings.Join(bar, ""), m.width),
		m.detail.view.View(),
		cut(footer, m.width),
	}, "\n")
}
