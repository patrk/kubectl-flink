# Contributing

Decisions are recorded in `docs/adr/` at the time they are made.

The code is pair-written with an AI assistant. Every design decision, every line, and every test must be reviewed and understood by the contributor before a PR is created. Contributions are welcome on the same terms: use whatever tools you like,
and be able to explain what you submit.

Before opening a PR:

    make lint test bench   # golangci-lint, race tests, benchmarks
    make kind-up e2e       # kind cluster with the operator, one job to RUNNING
    make fixtures          # capture CR and JobManager REST JSON from that job

`FLINK_OPERATOR_VERSION` and `FLINK_VERSION` select the versions; CI runs the grid of operator 1.15.0 and 1.16.1 with
Flink 1.20 and 2.2.

Conventions:

- The core is generic. Anything that knows about one platform, one connector or one label convention belongs in a profile or the rules file, not in code.
- `model.Job` is the single normalized object. `ls`, `why`, `report` and the TUI render from it and never fetch on their own.
- Every TUI feature exists as a CLI command first.
- Errors are returned, wrapped with `%w`, and checked with `errors.Is`.
- No interface with a single implementation unless a test needs to fake it.
