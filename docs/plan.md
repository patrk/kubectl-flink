# Plan

What v1 contains, how it is built, and in which order. Facts behind it are in [research](research.md); decisions are
in [ADRs](adr/).

## Commands

| Priority | Command | Notes |
|---|---|---|
| Must | `ls` | Health, filters, sort, `-o table\|wide\|json\|yaml\|name`, `-w`, fleet summary line |
| Must | `why <job>` | One-screen verdict, then evidence per layer. `--exit-code` for scripts |
| Must | `report <job>` | Debugging handover: `summary.md` plus a tarball, secrets redacted |
| Must | `exceptions <job>`, `checkpoints <job>`, `logs <job>` | Building blocks of `why`, usable on their own |
| Must | `top` | TUI over the same model |
| Must | `restart`, `suspend`, `resume` | Policy, owner warning, `--dry-run`, `--wait`, bulk rails |
| Must | `config view\|validate`, `version`, shell completion | Profile tooling |
| Should | `history <job>` | Reconstructed from events, managedFields, nonce timestamps, spec diffs |
| Should | `snapshots <job>`, `savepoint <job>` | FlinkStateSnapshot first, JobManager checkpoint history as fallback |
| Could | `ui <job>`, `ui --serve` | Browser escape hatch; the CLI and TUI are the primary interface |
| Could | `savepoint --restore-into-spec`, k9s `plugins.yaml` | |
| Should | Job graph tab in `top`: operators coloured by backpressure and busy time, as the Flink web UI does | The common look at one job without opening its web UI |
| Should | Argo CD sync and health of the Application that owns a job, in `ls -o wide` and `why` | Explains why a fix in git has not arrived |
| Later | Per-subtask checkpoint detail, TaskManager memory and threads, flame graphs, metric charts | Deep single-job inspection in the TUI |

Every TUI screen has a CLI equivalent with `-o json`. `<job>` is `[namespace/]name` or any unique substring of a name;
several matches list the candidates and exit 3.

## Architecture

```
cmd/              cobra commands, one file per verb, no logic beyond flags and output
internal/kube/    minimal typed CRD structs, typed REST client, informers per scope, RBAC fallback
internal/flink/   JobManager REST client (1.20 and 2.x), port-forward pool
internal/model/   Job (normalised FlinkDeployment | FlinkSessionJob), Health, derivation
internal/triage/  signals, rules engine, verdict, default rules (embedded YAML)
internal/ops/     write operations, policy, owner detection, bulk runner
internal/report/  bundle collection and redaction
internal/render/  table, json/yaml, markdown
internal/tui/     Bubble Tea app
internal/profile/ profile loading, defaults, validation
```

One data path: kube and flink sources → `model.Job` → triage → render or TUI. No command fetches on its own. `why`,
`report` and the TUI call the same collectors.

## Performance design

| Concern | Design |
|---|---|
| Large CRs (`lastReconciledSpec`, pod templates) | Minimal Go structs registered in a private scheme and decoded by a typed client. Unread fields are skipped by the decoder and never allocated. `managedFields` reduced to manager, operation and time |
| Listing | Informers start with WatchList (client-go v0.37, on by default; servers 1.34+ have it on), falling back to paginated LIST. One-shot `ls` uses paginated LIST (`limit` 250) with namespaces fetched in parallel |
| Watching | One informer per kind per scope. Pods only with the Flink component labels, reduced to phase, ready, restarts, last termination reason, node |
| Snapshots | Informer only when the profile is not stateless; index by `job-reference.name`, keep the newest completed per job |
| Events, HA ConfigMaps, logs | Fetched per job on demand with field or label selectors; never cached fleet-wide |
| JobManager REST | Never for lists. Detail data is lazy, one request set in flight, cancelled on cursor move, prefetched after 150 ms on the selected row. LRU of 4 open port-forwards |
| Redraw | Informer deltas coalesced into a 250 ms tick; every job is re-derived from the caches (under 1 ms for 1,500 jobs); only visible rows are rendered |
| Sort and filter | Precomputed keys per job, updated on change, so sorting 1,500 rows is a slice sort of small structs |

### Budgets

Measured in CI on synthetic data and checked against a kind cluster with the real operator.

| Budget | Target |
|---|---|
| `ls` first output, 500 jobs, one namespace | < 2 s |
| `ls`, 1,500 jobs across 4 namespaces | < 4 s |
| Resident memory, TUI, 1,500 jobs plus their pods | < 150 MB |
| TUI frame render at 1,500 rows | < 16 ms |
| Redraw rate under a burst of 1,000 updates | ≤ 4 per second |
| Detail pane after cursor stops | < 1 s with a warm port-forward |
| Binary size | < 40 MB |

## UX conventions

- kubectl flags via cli-runtime: `--context`, `-n`, `-A`, `--kubeconfig`, `--no-headers`. Colour only on a TTY,
  `NO_COLOR` honoured. Exit codes: 0 ok, 1 error, 2 usage, 3 ambiguous job, 4 unhealthy (with `--exit-code`).
- A context and namespace banner on every write and on `why` and `report`.
- Scopes from the profile (context plus namespaces). With several scopes, `ls` and `top` add a CONTEXT column, load
  scopes in parallel, and show a failing scope as an error row instead of failing the command.
- Forbidden at cluster scope falls back to the scopes' namespaces, then the context namespace, with one line saying
  why.
- `ls` columns: NAME, HEALTH, LIFECYCLE, JOB, JM, IN STATE, REASON (the worst finding, which carries the operator
  error when there is one). `-o wide` adds TM (ready/desired), JM RESTARTS, DRIFT, FLINK, IMAGE, OWNER, and SNAPSHOT
  AGE once snapshots are read. A summary line on stderr: jobs per health, pods, requested CPU and memory. `-o name`
  prints `namespace/name`, which every job argument accepts. `-w` prints changed rows only; `-o json -w` prints one
  event per line.
- Inside an SSH session nothing tries to open a browser; `ui` prints the tunnel command.
- TUI keys follow k9s: `/` search, `f` cycle health filter, `s` sort, `n` namespace, `c` context, `Enter` detail,
  `Esc` back, `?` help, `:` commands, `q` quit. First failing job's verdict reachable with `f`, `Enter`.

## Write operations

- `restart` sets `spec.restartNonce` to epoch milliseconds. `--hard` deletes the JobManager pod; `--reset-ha` also
  removes the HA ConfigMaps and requires `--force`.
- `suspend` and `resume` set `spec.job.state`. `savepoint` creates a FlinkStateSnapshot when the CRD is enabled,
  otherwise bumps the deprecated `savepointTriggerNonce` (legacy mode only).
- Every write: print the patch, `--dry-run=client|server`, field manager `kubectl-flink`, `--wait` on lifecycle and
  job state with a timeout.
- Policy per scope and operation: allow, confirm, block. Production confirmation asks the user to type
  `<namespace>/<name>`, or for bulk the namespace and the number of matched jobs, because pre-production and
  production can share a context.
- Owner detection from managedFields and profile rules. A job written by another manager gets a warning that names
  the owner and, if the profile knows it, the owner's own command.
- Bulk via `-l`, `--name-pattern`, `--pod-label`: `--batch N` (default 5), `--max-failures N` (default 1), `--wait`
  between batches, a summary at the end. `resume` has no `--all`.
- Restart of a stateless job states what the source will replay.
- The tool never restarts anything on its own.

## Milestones

Each milestone is a set of PRs from feature branches, merged with green CI. Tests ship with the code.

| # | Scope | Done when |
|---|---|---|
| M0 | `go.mod` (client-go v0.37, Kubernetes 1.34+ supported), Makefile, mise, lint, CI, PR template, goreleaser snapshot build, kind harness with operator 1.15.0 and 1.16.1 (charts from `archive.apache.org`, which keeps every patch release), fixture capture tool | CI green on an empty command; kind job reaches RUNNING in CI |
| M1 | Typed CRD structs and client, scopes and multi-context, RBAC fallback, `model.Job`, health derivation, profile loader, job matcher, `ls` with filters, sort, outputs, `-w`, summary line | Budgets for `ls` and memory met; health table tests cover every enum combination in the fixtures |
| M2 | Port-forward pool, REST client for 1.20 and 2.x, `exceptions`, `checkpoints`, `logs`, rules engine with default rules, `why` with operator, Kubernetes and runtime layers | Golden tests for each failure fixture; `why` on kind identifies induced failures |
| M2c | `why` performance layer (backpressure and its bottleneck, a sink without input), HA layer (leftover job plans and checkpoint pointers, several jobs on an application cluster), restart loops and missing restart strategy, external restarters through `spec.restartNonce` | Scenarios unreachable, restart-loop, restart-bot and ha on kind; synthetic cases for backpressure, idle sinks and HA leftovers |
| M3 | `report` with redaction, `history` | Redaction tests with secrets in every known location; bundle golden test |
| M4 | `top`: list, filters, search, sort, contexts, detail panes reusing M2 collectors, prefetch, job graph tab; then Argo CD status | teatest golden screens; frame and redraw budgets |
| M5 | Policy, owner detection, `restart`, `suspend`, `resume`, `savepoint`, `snapshots`, bulk runner, `--wait` | e2e on kind for each op including bulk and abort on failures; dry-run tests assert no writes |
| M6 | goreleaser release, custom krew index, krew-index submission, Homebrew tap, offline install, README | `kubectl krew install` from the custom index works on a clean machine |
| Later | `ui`, `ui --serve`, deep single-job inspection in the TUI | |

The TUI comes before write operations: triage is the core value, and writes carry the most risk.

## Tests

| Layer | How |
|---|---|
| Health, rules, redaction, matcher, profile | Table-driven unit tests on real CR and REST JSON captured from kind for each operator and Flink version |
| Kube layer | Fake typed client with informers; the 1,000 jobs and 10,000 snapshots test asserts memory and redraw rate |
| REST | `httptest` server replaying captured responses per Flink version |
| Output | Golden files for `ls`, `why`, `report`; teatest for TUI screens |
| End to end | kind with the real operator, grid of operator 1.15.0 / 1.16.1 and Flink 1.20 / 2.2. Induced failures: bad image, OOM, missing secret, failing job, stuck upgrade, HA leftovers. Writes with `--wait`, bulk, Forbidden at cluster scope |
| Performance | Benchmarks with budgets as assertions, run on every PR |

Lint, unit and performance tests run on every PR; the e2e grid runs on every PR touching `internal/kube`, `internal/ops`
or `internal/flink`, and nightly.

## Risks

| Risk | Mitigation |
|---|---|
| LIST transfer for large CRs dominates `ls` time | Measure in M1 against 1,500 real CRs on kind; parallel namespaces; `-w` and TUI keep a warm cache |
| Field differences between operator versions | Minimal structs with optional fields; fixtures per version |
| Port-forward through the API server under load | One request set at a time, LRU reuse, timeouts with a clear message |
| Memory on small jumphosts | Budgets enforced in CI; pods informer only where needed |
| Bubble Tea API changes | Pin versions; TUI isolated in `internal/tui` |
