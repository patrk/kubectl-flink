// Package version holds build information set by the linker.
package version

// Set with -ldflags "-X github.com/patrk/kubectl-flink/internal/version.Version=...".
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)
