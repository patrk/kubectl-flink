# 1. Name and scope

The repository is `kubectl-flink`, the binary `kubectl-flink`, the Go module `github.com/patrk/kubectl-flink`, and the
krew plugin name `flink`, so the tool is invoked as `kubectl flink`. Krew's naming guide asks for a specific name with
the product as its prefix and for repositories that start with `kubectl-`. Krew-index already carries plugins named
after the product they serve (`cnpg`, `rabbitmq`, `kueue`, `ray`, `kyverno`) and had no `flink` plugin when this was
decided. People looking for this tool search for "kubectl flink", so the name matches the search. A branded name would
have to be explained first.

Version 1 reads, explains and reports on Flink Kubernetes Operator resources, and covers savepoint, restart, suspend and
resume, the imperative operations the CRD makes awkward. Job submission, spec editing, autoscaler controls and vendor
distributions are out of v1. Deep inspection of a single job (job graphs, flame graphs, thread dumps, SQL) is deferred
until fleet triage is done, not ruled out. GitOps owns the desired state. Platform-specific behaviour lives in a profile file and a
rules file so that the core stays usable by anyone running the Apache operator.
