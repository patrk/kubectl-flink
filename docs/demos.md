# Demos

Recorded against the operator on kind with the example jobs of `make demo`; nothing is edited.

## The classic way, then one command

`kubectl get` shows no health, `describe` keeps the cause in a JSON string, and the log ends in stack frames. `kubectl flink` lists what is unhealthy and names the cause with its evidence.

<picture>
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/patrk/kubectl-flink/releases/download/demo/classic-light.gif">
  <img alt="The classic way, then one command" src="https://github.com/patrk/kubectl-flink/releases/download/demo/classic.gif">
</picture>

## A crash loop, from alert to handover

`top` filters to the unhealthy jobs and opens one: its verdict, then the log of the container before the last crash, then `R` writes the handover bundle whose `summary.md` goes into the ticket.

<picture>
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/patrk/kubectl-flink/releases/download/demo/crashloop-light.gif">
  <img alt="A crash loop, from alert to handover" src="https://github.com/patrk/kubectl-flink/releases/download/demo/crashloop.gif">
</picture>

## Where a job's time goes

Each operator's busy, back-pressured and idle time and its rates, coloured as the Flink web UI colours its job graph, as text or as JSON.

<picture>
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/patrk/kubectl-flink/releases/download/demo/graph-light.gif">
  <img alt="Where a job's time goes" src="https://github.com/patrk/kubectl-flink/releases/download/demo/graph.gif">
</picture>

## Jobs by the IDs people use

A use-case ID in a label, a team label, and `why` on an ID in another namespace.

<picture>
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/patrk/kubectl-flink/releases/download/demo/ids-light.gif">
  <img alt="Jobs by the IDs people use" src="https://github.com/patrk/kubectl-flink/releases/download/demo/ids.gif">
</picture>


## At scale

`make demo flood` adds 2,000 FlinkDeployments in 40 namespaces to the example jobs. Their specs and statuses are
copied from jobs the operator really ran, so each resource has a real size; the operator ignores them, so no
Flink cluster runs behind them. `make bench-flood` times the same questions with plain kubectl and with
kubectl flink, with [hyperfine](https://github.com/sharkdp/hyperfine), on kind on a laptop. A remote cluster adds
its round trips to both.

2009 FlinkDeployments, 2026-10-25, hyperfine 2.0.0; mean ± σ of 20 runs after 3 warmups;
profile idLabels: [use-case].

| Command | Mean Wall Time [ms] | Change | Factor |
|:---|---:|---:|:---|
| `kubectl get -A` | 234.3 ± 5.3 |  |  |
| `kubectl flink ls -A` | 147.2 ± 4.3 | -37.2% | (1.6x faster) |

| Command | Mean Wall Time [ms] | Change | Factor |
|:---|---:|---:|:---|
| `kubectl get -A -o json \| jq (unhealthy)` | 855.0 ± 5.4 |  |  |
| `kubectl flink ls -A --unhealthy` | 148.4 ± 6.1 | -82.6% | (5.8x faster) |

| Command | Mean Wall Time [ms] | Change | Factor |
|:---|---:|---:|:---|
| `kubectl get -A -l use-case=1234` | 34.3 ± 2.2 |  |  |
| `kubectl flink ls -A 1234` | 22.5 ± 1.7 | -34.5% | (1.5x faster) |

| Command | Mean Wall Time [ms] | Change | Factor |
|:---|---:|---:|:---|
| `kubectl flink why 1234 (-A)` | 28.7 ± 4.5 |  |  |

- Listing with a health value per job is faster than `kubectl get` without one: each page is decoded while the
  API server encodes the next, and the table is written in a few large writes.
- Finding the unhealthy jobs with kubectl alone means fetching every resource as JSON and filtering it, and the
  filter only sees lifecycle and errors, not restarts, missing TaskManagers or stale reconciles.
- An ID is looked up by the API server in the profile's ID labels, and only the pods of the jobs found are read.
  Without idLabels in the profile, or when no job carries the value, every job is read, to match it in any label,
  on the pod template too, or in the name; that costs what `ls -A` costs.

## Time to an answer

Wall time is the smaller part. The larger one is how many steps a question takes, and what you must already know
to take them.

| Question | kubectl | kubectl flink |
|:---|:---|:---|
| Which of 2,000 jobs need attention? | `get -A` shows lifecycle and job state; restarts, missing TaskManagers and stale reconciles need a `describe` per job | `ls -A --unhealthy`, or `top`: one health value per job, the worst first |
| Why is this one failing? | `describe` for `status.error` in a JSON string, `get pods`, `logs --previous` on the right container, a port-forward to the web UI for exceptions: five steps, and the pod and container names | `why <job>`: the cause first, then the evidence by layer; 26 ms here for a crash loop, 43 ms for a running job asked over REST, 30 ms by ID across 2,009 jobs |
| Which job is use case 1234? | `get -A -l use-case=1234`, if you know the label key | `ls -A 1234`, or `/1234` in `top`: any ID label, or the name |
| What goes into the ticket? | Copy and paste the above, and remove the secrets by hand | `report <job>`, or `R` in `top`: one bundle, secrets redacted |
