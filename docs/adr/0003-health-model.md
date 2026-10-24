# 3. One derived health value

`lifecycleState`, `jobStatus.state` and `jobManagerDeploymentStatus` each mislead on their own: STABLE does not mean
RUNNING, and READY says nothing about the job. The tool derives one value, FAILED, STUCK, DEGRADED, PROGRESSING,
SUSPENDED or OK, from all three plus drift, `status.error`, TaskManager counts and pod states. The derivation
is a pure function with table tests over every combination found in captured fixtures, and `ls`, filters, sorting,
`why` and the TUI all use it.

Time-based rules are relative to the operator's effective reconcile interval, which the profile can set and which
defaults to the operator's 60 seconds. `status.error` carries no timestamp. On a FlinkDeployment the operator clears
it on the next healthy observe, so it makes the job DEGRADED; on a FlinkSessionJob it persists until the next spec
change, so under STABLE it makes the job STUCK. Thresholds are never stricter than the operator's own. A transition
within its threshold is PROGRESSING, so a starting or upgrading job is neither hidden as OK nor reported as broken; a
JobManager ERROR right after submit, which the operator produces while it creates the config ConfigMap, stays
PROGRESSING for a grace period.
