package report

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/patrk/kubectl-flink/internal/history"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/render"
	"github.com/patrk/kubectl-flink/internal/triage"
)

// SummaryInput is what summary.md is written from.
type SummaryInput struct {
	Job      render.JobRef
	Health   string
	Reasons  []string
	Verdict  *triage.Verdict
	History  *history.History
	Captured time.Time
	Version  string
}

// maxTimeline bounds the timeline in the summary; history.txt has all of it.
const maxTimeline = 15

// Summary writes summary.md: what a colleague needs first, with pointers into the bundle.
func (b *Bundle) Summary(in *SummaryInput) string {
	var s strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&s, format+"\n", args...) }
	p("# %s %s/%s", in.Job.Kind, in.Job.Namespace, in.Job.Name)
	p("")
	p("Context `%s`, captured %s by kubectl-flink %s.", in.Job.Context, in.Captured.UTC().Format(time.RFC3339), in.Version)
	if in.Job.JobID != "" {
		p("Flink job `%s`.", in.Job.JobID)
	}
	p("")
	p("**Health: %s**", in.Health)
	for _, r := range in.Reasons {
		p("- %s", r)
	}
	p("")
	p("## Matched patterns")
	p("")
	if len(in.Verdict.Findings) == 0 {
		p("None of the known patterns matches.")
	}
	for i := range in.Verdict.Findings {
		f := &in.Verdict.Findings[i]
		p("%d. **%s**: %s (%s)", i+1, f.Severity, f.Title, f.Layer)
		p("   %s", f.Explain)
		switch {
		case strings.HasPrefix(f.Remedy, "kubectl "):
			p("   Next: `%s`", f.Remedy)
		case f.Remedy != "":
			p("   Next: %s", f.Remedy)
		}
	}
	if len(in.Verdict.Unexplained) > 0 {
		p("")
		// The same choice as why's output, so that summary.md and why.txt agree.
		if in.Verdict.Health >= model.Degraded && !in.Verdict.Explained {
			p("Not explained by a known pattern; look first at:")
		} else {
			p("Also seen, not explained by the above:")
		}
		for _, sig := range in.Verdict.Unexplained {
			p("- %s: %s", sig.Kind, oneLine(sig.Text, 200))
		}
	}
	p("")
	p("## Timeline, newest first")
	p("")
	p("| UTC | Source | What |")
	p("|---|---|---|")
	for i, e := range in.History.Entries {
		if i == maxTimeline {
			p("| … | | %d more in history.txt |", len(in.History.Entries)-maxTimeline)
			break
		}
		p("| %s | %s | %s |", e.At.UTC().Format("01-02 15:04:05"), e.Source, strings.ReplaceAll(oneLine(e.Text, 140), "|", "\\|"))
	}
	if len(in.History.Pending) > 0 {
		p("")
		p("## Not applied yet")
		p("")
		for _, c := range in.History.Pending {
			p("- `%s`: %s -> %s", c.Field, oneLine(c.From, 80), oneLine(c.To, 80))
		}
	}
	if len(b.Gaps) > 0 {
		p("")
		p("## Not collected")
		p("")
		for _, g := range b.Gaps {
			p("- %s", oneLine(g, 200))
		}
	}
	p("")
	p("## Files")
	p("")
	p("| File | Values redacted |")
	p("|---|---|")
	for _, f := range b.Files {
		p("| %s | %d |", f.Name, f.Redacted)
	}
	p("")
	p("Values under keys that name passwords, tokens and keys, and secrets in URLs, JAAS and Bearer headers, are " +
		"replaced with `[REDACTED]`. Check the files before sharing them outside your team.")
	return s.String()
}

func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// WriteDir writes the bundle into dir.
func (b *Bundle) WriteDir(dir string) error {
	for _, f := range b.Files {
		path := filepath.Join(dir, filepath.FromSlash(f.Name))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return err
		}
		if err := os.WriteFile(path, f.Data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// WriteTarGz writes the bundle as a gzipped tar under top/, reproducibly: files in name order, all with the
// capture time and fixed owner and mode.
func (b *Bundle) WriteTarGz(w io.Writer, top string, mtime time.Time) error {
	gz := gzip.NewWriter(w)
	gz.ModTime = mtime
	tw := tar.NewWriter(gz)
	for _, f := range b.Files {
		hdr := &tar.Header{Name: top + "/" + f.Name, Mode: 0o644, Size: int64(len(f.Data)), ModTime: mtime, Format: tar.FormatPAX}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(f.Data); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// Size is the bundle's uncompressed size in bytes.
func (b *Bundle) Size() int {
	n := 0
	for _, f := range b.Files {
		n += len(f.Data)
	}
	return n
}
