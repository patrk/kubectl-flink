package tui

import (
	"context"
	"errors"
	"io"

	tea "charm.land/bubbletea/v2"
)

// Run shows top on out, reading keys from in, until the user quits or ctx ends.
func Run(ctx context.Context, cfg Config, in io.Reader, out io.Writer) error {
	final, err := tea.NewProgram(New(cfg), tea.WithContext(ctx), tea.WithInput(in), tea.WithOutput(out)).Run()
	if errors.Is(err, tea.ErrInterrupted) || errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
		err = nil // Ctrl+C or SIGINT: quitting, as q does
	}
	if m, ok := final.(*Model); ok && err == nil {
		err = m.err
	}
	return err
}
