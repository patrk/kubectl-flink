# kubectl flink

A kubectl plugin and terminal UI (TUI) for the [Apache Flink Kubernetes Operator](https://github.com/apache/flink-kubernetes-operator).
It shows the health of every `FlinkDeployment` and `FlinkSessionJob`, explains why a Flink job is failing, and handles
savepoints, restarts, suspend and resume safely, across hundreds of jobs and several clusters. Installs through krew.

`kubectl get flinkdeployments` shows no health, no drift and no `status.error`. `kubectl flink` derives one health
value per job and explains a failure layer by layer: Flink runtime (exceptions, restarts), Kubernetes (pending pods,
OOMKilled, events), performance (backpressure, checkpoints, source lag) and operator (lifecycle, reconciliation,
rollbacks).

> **Status: early development.** `ls` works; the other commands below are the working draft. Build with `make build`.

```
kubectl flink ls [-A] [FILTER] [--unhealthy | --health failed,stuck] [-l selector] [--sort health|name|state-age]
                [-o wide|json|yaml|name] [-w] [--exit-code]
kubectl flink why <job>            # one-screen verdict, then the raw evidence
kubectl flink report <job>         # handover bundle: every fact needed to debug the job, secrets redacted
kubectl flink exceptions | checkpoints | snapshots | logs | history <job>
kubectl flink ui <job>             # port-forward to the JobManager and open the Flink web UI
kubectl flink savepoint | restart | suspend | resume <job> [--dry-run] [--wait]
kubectl flink top                  # k9s-style terminal UI on the same data
```

Design goals: fast at fleet scale (first rows in under 2 s at 500 jobs, no network on redraw), few keystrokes to an
answer, safe write operations (dry run, prod confirmation, wait on lifecycle), and a generic core with platform
specifics in a profile file.

Not in v1: job submission, spec editing, autoscaler controls. Deep single-job inspection (job graphs, flame graphs,
SQL) follows once fleet triage is done.

Apache License 2.0. Not affiliated with the Apache Software Foundation; Apache Flink is its trademark.
