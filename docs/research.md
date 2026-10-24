# Research

Facts the design depends on, checked against source. Operator `release-1.15.0` and `release-1.16.1`, Flink
`release-1.20.5` and `release-2.2.1`. Paths are relative to those repositories. The last section lists the operational
situations the design must handle.

## Operator reference

### Status model

| Field | Values | Notes |
|---|---|---|
| `status.lifecycleState` | CREATED, SUSPENDED, UPGRADING, DEPLOYED, STABLE, ROLLING_BACK, ROLLED_BACK, FAILED, DELETING, DELETED | Derived on every reconcile, see below |
| `status.jobStatus.state` | Flink `JobStatus`: INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDED, RECONCILING | The operator writes RECONCILING itself when it cannot observe the job (job not found, JM missing or failing, during submit) |
| `status.jobManagerDeploymentStatus` | READY, DEPLOYED_NOT_READY, DEPLOYING, MISSING, ERROR | FlinkDeployment only. Default MISSING. Only READY means REST is reachable |
| `status.reconciliationStatus.state` | DEPLOYED, UPGRADING, ROLLING_BACK, ROLLED_BACK | Default UPGRADING |
| `status.conditions` | one condition, type `Running` | FlinkDeployment only. Application mode: True iff job RUNNING. Session mode: True iff JM READY |
| `status.error` | string | See below |
| `status.observedGeneration` | int64 | Drift when `< metadata.generation` |
| `status.taskManager` | `labelSelector`, `replicas` | Backs the `/scale` subresource |

`FlinkSessionJobStatus` has only `jobStatus`, `error`, `observedGeneration`, `lifecycleState`, `reconciliationStatus`.
`jobStatus.savepointInfo` and `checkpointInfo` are deprecated and filled only when FlinkStateSnapshot is disabled.

`CommonStatus.getLifecycleState()` (identical in 1.15 and 1.16), in order:

1. DELETING or DELETED if stored; these are never computed.
2. Before first deploy: CREATED, or FAILED if `error` is set.
3. Reconciliation state UPGRADING or ROLLING_BACK passes through.
4. SUSPENDED if the last reconciled spec has `job.state: suspended`.
5. FAILED if `jobStatus.state` is FAILED, or (FlinkDeployment) the JM is MISSING or ERROR and `error` contains one of
   "Manual restore required.", "It is possible that the job has finished or terminally failed, or the configmaps have
   been deleted.", "HA metadata is not available".
6. ROLLED_BACK if the reconciliation state says so.
7. STABLE if `lastReconciledSpec == lastStableSpec`, otherwise DEPLOYED.

Consequence: STABLE does not mean RUNNING. A job can be STABLE with `jobStatus.state` RESTARTING or RECONCILING, so
health must combine the three enums.

### `status.error`

- Usually JSON of `FlinkResourceException`: `type`, `message`, `stackTrace` (only when enabled), `throwableList`
  (cause chain, capped, no nested stack traces), `additionalMetadata` (`httpResponseCode`, `reason` such as
  CrashLoopBackOff or FailedMount, `labels`).
- Plain string `Job Not Found` when the job is missing, and `e.toString()` if serialisation fails. Parsers must accept
  all three forms.
- Cleared on every spec reconcile, and for FlinkDeployment on every observe when the JM is not ERROR, the job is not
  FAILED and the spec is stable. A FlinkDeployment error under STABLE therefore lasts about one reconcile interval; a
  FlinkSessionJob error persists until the next spec change.
- With the validating webhook disabled, invalid specs surface only here.

### Printer columns

FlinkDeployment and FlinkSessionJob print Job Status and Lifecycle State. FlinkStateSnapshot prints Path, Result
Timestamp and Snapshot State. Short names `flinkdep`, `sessionjob`, `flinksnp`. Nothing shows JM status, drift, error,
restarts or snapshot age.

### FlinkStateSnapshot

- Spec: `jobReference{kind, name}`, `savepoint{path, formatType, disposeOnDelete, alreadyExists}` or `checkpoint{}`,
  `backoffLimit`. Status: `state` (TRIGGER_PENDING, IN_PROGRESS, COMPLETED, FAILED, ABANDONED), `path`, `triggerId`,
  `triggerTimestamp`, `resultTimestamp`, `error`, `failures`.
- Enabled by default (`kubernetes.operator.snapshot.resource.enabled: true`). No ownerReferences.
- Labels set by the controller on every reconcile: `snapshot.type`, `snapshot.trigger-type`
  (MANUAL, PERIODIC, UPGRADE, UNKNOWN), `snapshot.state`, `job-reference.kind`, `job-reference.name`.
- Latest completed per job: one list with `-l job-reference.name=<job>,snapshot.state=COMPLETED`, max
  `resultTimestamp`. The `snapshot.state` label can lag `.status.state` briefly.
- Savepoints taken directly through the JobManager REST API never appear here.

### Events

Reasons emitted as Warning: SavepointError, CheckpointError, SnapshotError, SnapshotAbandoned, JobException, Missing,
Error, ValidationError, UnsupportedFlinkVersion, CleanupFailed, RecoverDeployment, RestartUnhealthyJob,
ResourceQuotaReached, AutoscalerError, ScalingLimited, ScalingVetoed, and container reasons passed through
(CrashLoopBackOff, ImagePullBackOff, ErrImagePull, FailedMount).

### Configuration that sets thresholds

| Key (`kubernetes.operator.`) | Code default | Helm chart value |
|---|---|---|
| `reconcile.interval` | 60s | 15 s |
| `observer.progress-check.interval` | 10s | 5 s |
| `cluster.health-check.enabled` | false | |
| `cluster.health-check.restarts.threshold` / `.window` | 64 / 2 min | |
| `job.restart.failed` | false | |
| `deployment.rollback.enabled` | false | |
| `deployment.readiness.timeout` | 5 min | |
| `periodic.savepoint.interval`, `periodic.checkpoint.interval` | off | |
| `savepoint.history.max.count` / `.max.age` | 10 / 24h | |

The chart's 15 s and 5 s live in its default `defaultConfiguration` block; a platform that overrides that block falls
back to the code defaults (observed: 60 s). Both are also settable per resource in `spec.flinkConfiguration`. Health
thresholds must be read relative to the effective reconcile interval, not hard-coded.

### JobManager REST service

`<metadata.name>-rest`, port name `rest`, port 8081, for native and standalone mode. Clusters short of service IPs set
`kubernetes.rest-service.exposed.type: Headless_ClusterIP`, so resolve through the service selector or endpoints, never
assume a ClusterIP. The operator itself connects to `http://<name>-rest.<ns>:8081`.

### Flink REST, 1.20 and 2.2

| Endpoint | Fields used | 1.20 vs 2.2 |
|---|---|---|
| `/jobs/overview` | `jid, name, state, start-time, duration, tasks` | 2.2 adds `pending-operators` |
| `/jobs/:jid/exceptions?maxExceptions=N` | `exceptionHistory.entries[]{exceptionName, stacktrace, timestamp, taskName, endpoint, taskManagerId, failureLabels, concurrentExceptions}`, `exceptionHistory.truncated` | 2.2 removed top-level `root-exception`, `all-exceptions`, `timestamp`; read `exceptionHistory` only |
| `/jobs/:jid/checkpoints` | `counts`, `summary{state_size, end_to_end_duration, …}` with min/max/avg/p50…p999, `latest{completed, savepoint, failed, restored}`, `history[]` incl. `failure_message` | same |
| `/jobs/:jid/checkpoints/config` | `interval, timeout, mode, externalization, checkpoint_storage, tolerable_failed_checkpoints` | same |
| `/jobs/:jid/plan` | `nodes[]{id, description, parallelism}` | typed in 2.2, raw JSON in 1.20; same shape |
| `/jobs/:jid/vertices/:vid/backpressure` | `backpressureLevel` (`ok`, `low`, `high`), `subtasks[]{ratio, busyRatio, idleRatio}` | same; legacy `backpressure-level` key still emitted |
| `/jobs/:jid/vertices/:vid/subtasks/metrics?get=…&agg=…` | `[{id, min, max, avg, sum, skew}]`; one unknown name empties the whole answer | same |
| `/jobs/:jid` | `plan.nodes[]{id, inputs[]{id}}`: sources have no inputs, sinks feed no node | same |

`/jobs/:jid/vertices/:vid/metrics` is deprecated but still present in 2.2.

A sink task emits nothing downstream, so its `numRecordsOutPerSecond` is 0 on a healthy job; what reaches a sink is
its `numRecordsInPerSecond`. A job without checkpointing and without `restart-strategy.type` has no restart strategy:
the first task failure fails the job ("Recovery is suppressed by NoRestartBackoffTimeStrategy").

FLIP-33: `pendingRecords` exists only when the connector registers it. The Kafka source does, lazily after the first
fetch per partition, and does not register `currentFetchEventTimeLag`. Treat both as optional.

### Kubernetes HA ConfigMaps

Checked on kind with Flink 1.20 and 2.2. `<name>-cluster-config-map` holds the leader entries
(`org.apache.flink.k8s.leader.*`) and one job plan per job: `executionPlan-<jid>` on 2.x, `jobGraph-<jid>` on 1.x.
`<name>-<jid>-config-map` holds the job's checkpoint pointers (`checkpointID-…`, `counter`). Both carry
`app=<name>` and `type=flink-native-kubernetes`; only 1.20 also sets `configmap-type: high-availability`. The job
result store lives in `high-availability.storageDir`, outside Kubernetes.

The operator records spec changes as Normal `SpecChanged` events with the diff, such as
`Diff: FlinkDeploymentSpec[job.state : running -> suspended]`; managedFields name who wrote each top-level spec field.

### Kubernetes client

WatchList (KEP-3157) is Beta, not GA: server gate on from 1.34, off in 1.33. client-go `WatchListClient` defaults to
on from v0.35. Use client-go v0.35 or later and keep the paginated LIST fallback.

## Design assumptions the research contradicts

| Draft assumption | Finding | Change |
|---|---|---|
| GitOps owns the desired state | A runtime service may own the CRs, with Argo told to ignore them | External lifecycle owner detection and write-op policy |
| `-A` works, degrading on Forbidden | Cluster-scope list is commonly Forbidden | Per-namespace is the normal path |
| Bulk ops by CR label | CRs may carry no business labels | Name-pattern and pod-template-label selectors |
| STUCK when `status.error` is set while STABLE | Transient on FlinkDeployment, sticky on FlinkSessionJob | Grace period of one reconcile interval, per kind |
| `restart --hard` is dangerous without HA | Stale HA metadata is itself a failure cause | Guard on HA state, show it in `why` |
| Snapshots come from FlinkStateSnapshot | Savepoints may be taken via REST only | Fall back to JobManager checkpoint history |
| Flink 2.x removed `vertices/:vid/metrics` | It removed top-level `root-exception` instead | `exceptions` reads `exceptionHistory` |
| WatchList is GA | Beta; default-on in client-go v0.35 | Keep LIST fallback |
| Lifecycle and reconciliation enums as drafted | Missing DELETING and ROLLED_BACK | Full enums |
| `report` attaches to tickets | Support runs in chat | `report` is a debugging handover bundle |
| Chart reconcile interval 15 s | Code default 60 s when the chart block is overridden | Thresholds relative to the effective interval |

## Operational situations

Situations common on large operator installations, and what each means for the tool.

| Situation | Consequence |
|---|---|
| Another controller or service owns the CRs; GitOps may ignore them | Detect an external lifecycle owner (managedFields, profile rule). Write operations warn, name the owner's own command, and follow a configurable allow/confirm/block policy |
| Other controllers restart jobs by bumping `spec.restartNonce` | `why` and `history` show generation churn, the nonce timeline and which manager wrote it. The tool never restarts on its own. `restart` writes an epoch-millis nonce |
| Stale Kubernetes HA metadata makes a JobManager recover old job plans | An HA layer in `why`: HA ConfigMap, `executionPlan-*` key count, job-result-store state. `restart --hard` guarded on it |
| A JobManager restart leaves TaskManagers behind, or recovers a second job | TaskManager pods actual vs expected; more than one job in `/jobs/overview` |
| CRs carry no owner or tenant labels; business IDs sit in names or pod-template labels | Selectors by name pattern and `spec.podTemplate` labels. Fuzzy `<job>` matching on substrings |
| Users cannot list Flink resources at cluster scope | Per-namespace operation is the normal case; namespaces from the context, a flag or the profile |
| Production and pre-production share a cluster | Production detection by context pattern, namespace pattern or namespace label |
| Real checkpoint and savepoint directories are injected at pod start | Read effective config from the JobManager, not the CR |
| Most jobs are stateless; savepoints are taken over REST | Snapshot columns optional; `snapshots` falls back to JobManager checkpoint history; restart warns what the source will replay |
| A spec change redeploys many jobs at once | `history` shows the triggering diff; `ls --since-change` |
| No alerting on derived job health | `ls -o json` exposes the health value |
| Production is reachable only from a jumphost | Low memory footprint. `ui` binds a fixed port and prints the SSH tunnel command. Offline krew install from manifest plus archive |
| Validating webhook disabled | Spec errors only in `status.error`; ERROR column |
| Failures from admission policies, image pulls, scheduling, OOM, network policies and connection limits | Default verdict rules over events, pod states and exception signatures, extendable by profile |
