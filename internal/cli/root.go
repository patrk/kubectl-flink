// Package cli wires the cobra commands. Commands parse flags and render; they hold no logic of their own.
package cli

import (
	"errors"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
)

// Exit codes shared by all commands.
const (
	ExitOK        = 0
	ExitError     = 1
	ExitUsage     = 2
	ExitAmbiguous = 3
	ExitUnhealthy = 4
)

// exitError carries an exit code through cobra's error return.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

// Options are shared by every command.
type Options struct {
	ConfigFlags *genericclioptions.ConfigFlags
	genericiooptions.IOStreams
}

// NewRootCommand builds the command tree.
func NewRootCommand(streams genericiooptions.IOStreams) *cobra.Command {
	o := &Options{
		ConfigFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
	cmd := &cobra.Command{
		Use:           "kubectl-flink",
		Short:         "Triage and operate Apache Flink Kubernetes Operator jobs",
		SilenceUsage:  true,
		SilenceErrors: true,
		Annotations: map[string]string{
			cobra.CommandDisplayNameAnnotation: "kubectl flink",
		},
	}
	cmd.SetIn(streams.In)
	cmd.SetOut(streams.Out)
	cmd.SetErr(streams.ErrOut)
	o.ConfigFlags.AddFlags(cmd.PersistentFlags())
	cmd.AddCommand(newVersionCommand(o))
	markUsageErrors(cmd)
	return cmd
}

// markUsageErrors makes flag and argument errors of every command exit with ExitUsage.
// The root rejects arguments and is runnable, so that cobra validates them and an unknown command is an
// argument error too; without arguments it prints help.
func markUsageErrors(root *cobra.Command) {
	root.Args = cobra.NoArgs
	root.RunE = func(c *cobra.Command, _ []string) error { return c.Help() }
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &exitError{code: ExitUsage, err: err}
	})
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if args := c.Args; args != nil {
			c.Args = func(c *cobra.Command, a []string) error {
				if err := args(c, a); err != nil {
					return &exitError{code: ExitUsage, err: err}
				}
				return nil
			}
		}
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(root)
}

// Execute runs the root command against the process streams and returns the exit code.
func Execute() int {
	streams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	return run(NewRootCommand(streams), streams)
}

func run(cmd *cobra.Command, streams genericiooptions.IOStreams) int {
	err := cmd.Execute()
	if err == nil {
		return ExitOK
	}
	_, _ = streams.ErrOut.Write([]byte("error: " + err.Error() + "\n"))
	var ee *exitError
	if errors.As(err, &ee) {
		return ee.code
	}
	return ExitError
}
