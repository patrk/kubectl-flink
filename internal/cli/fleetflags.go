package cli

import (
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

// fleetFlags are the flags ls and top share: which jobs, and in what order.
type fleetFlags struct {
	allNamespaces bool
	selector      string
	health        []string
	unhealthy     bool
	sortBy        string

	// healthValues is --health and --unhealthy, parsed.
	healthValues []model.Health
}

func (ff *fleetFlags) register(cmd *cobra.Command) {
	f := cmd.Flags()
	f.BoolVarP(&ff.allNamespaces, "all-namespaces", "A", false, "All namespaces, falling back to the profile's namespaces when that is forbidden")
	f.StringVarP(&ff.selector, "selector", "l", "", "Label selector on the FlinkDeployments and FlinkSessionJobs")
	f.StringSliceVar(&ff.health, "health", nil, "Only jobs with these health values, such as failed,stuck")
	f.BoolVar(&ff.unhealthy, "unhealthy", false, "Only DEGRADED, STUCK and FAILED jobs")
	f.StringVar(&ff.sortBy, "sort", "health", "Sort by: "+strings.Join(fleet.SortKeys, ", "))
	_ = cmd.RegisterFlagCompletionFunc("sort", cobra.FixedCompletions(fleet.SortKeys, cobra.ShellCompDirectiveNoFileComp))
}

// parse checks the sort key and parses the health values.
func (ff *fleetFlags) parse() error {
	if !slices.Contains(fleet.SortKeys, ff.sortBy) {
		return fmt.Errorf("unknown sort key %q, use one of %s", ff.sortBy, strings.Join(fleet.SortKeys, ", "))
	}
	ff.healthValues = nil
	for _, s := range ff.health {
		h, err := model.ParseHealth(s)
		if err != nil {
			return err
		}
		ff.healthValues = append(ff.healthValues, h)
	}
	if ff.unhealthy {
		ff.healthValues = append(ff.healthValues, fleet.UnhealthyValues()...)
	}
	return nil
}
