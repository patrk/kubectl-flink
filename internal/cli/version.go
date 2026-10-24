package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/version"
)

func newVersionCommand(o *Options) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the plugin version",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			info := map[string]string{"version": version.Version, "commit": version.Commit, "date": version.Date}
			switch output {
			case "":
				_, err := fmt.Fprintf(o.Out, "kubectl-flink %s (%s, %s)\n", version.Version, version.Commit, version.Date)
				return err
			case "json":
				enc := json.NewEncoder(o.Out)
				enc.SetIndent("", "  ")
				return enc.Encode(info)
			default:
				return &exitError{code: ExitUsage, err: fmt.Errorf("unsupported output %q, use json", output)}
			}
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format: json")
	return cmd
}
