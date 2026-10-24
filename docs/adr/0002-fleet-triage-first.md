# 2. Fleet triage first, CLI and TUI end to end

The first version answers three questions across hundreds of jobs: which are unhealthy, why, and what can safely be
done about it. Everything is reachable from the CLI and the TUI, so an operator never has to leave the terminal; the
Flink web UI stays available through `ui` but is not the primary interface. Every TUI screen has a CLI command with
`-o json`.

Deep inspection of a single job (job graph, per-subtask checkpoint detail, TaskManager memory and threads, flame
graphs, metric charts) follows once triage is complete. It reads the same JobManager endpoints through the same
port-forward pool, so it adds screens rather than plumbing. Performance and keystroke count are requirements with
budgets enforced in CI, not goals.
