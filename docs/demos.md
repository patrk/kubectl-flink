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

2009 FlinkDeployments, 2026-10-25, hyperfine 2.0.0; mean ± σ of 20 runs after 3 warmups.

| Command | Mean Wall Time [ms] | Change | Factor |
|:---|---:|---:|:---|
| `kubectl get -A` | 231.6 ± 4.2 |  |  |
| `kubectl flink ls -A` | 197.5 ± 7.5 | -14.7% |  |

| Command | Mean Wall Time [ms] | Change | Factor |
|:---|---:|---:|:---|
| `kubectl get -A -o json \| jq (unhealthy)` | 847.0 ± 10.1 |  |  |
| `kubectl flink ls -A --unhealthy` | 153.1 ± 7.8 | -81.9% | (5.5x faster) |

| Command | Mean Wall Time [ms] | Change | Factor |
|:---|---:|---:|:---|
| `kubectl get -A -l use-case=1234` | 35.0 ± 2.6 |  |  |
| `kubectl flink ls -A 1234` | 152.9 ± 6.8 | +337.0% | (4.4x slower) |

- Listing with a health value per job costs no more than `kubectl get` without one.
- Finding the unhealthy jobs with kubectl alone means fetching every resource as JSON and filtering it, and the
  filter only sees lifecycle and errors, not restarts, missing TaskManagers or stale reconciles.
- Finding a job by one known label is faster with kubectl: the API server filters by the label. kubectl flink
  reads every job, to match the value in any ID label, on the pod template too, or in the name.

## Time to an answer

Wall time is the smaller part. The larger one is how many steps a question takes, and what you must already know
to take them.

| Question | kubectl | kubectl flink |
|:---|:---|:---|
| Which of 2,000 jobs need attention? | `get -A` shows lifecycle and job state; restarts, missing TaskManagers and stale reconciles need a `describe` per job | `ls -A --unhealthy`, or `top`: one health value per job, the worst first |
| Why is this one failing? | `describe` for `status.error` in a JSON string, `get pods`, `logs --previous` on the right container, a port-forward to the web UI for exceptions: five steps, and the pod and container names | `why <job>`: the cause first, then the evidence by layer; 26 ms here for a crash loop, 43 ms for a running job asked over REST |
| Which job is use case 1234? | `get -A -l use-case=1234`, if you know the label key | `ls -A 1234`, or `/1234` in `top`: any ID label, or the name |
| What goes into the ticket? | Copy and paste the above, and remove the secrets by hand | `report <job>`, or `R` in `top`: one bundle, secrets redacted |
