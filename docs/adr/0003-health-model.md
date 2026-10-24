# 3. One derived health value

`lifecycleState`, `jobStatus.state` and `jobManagerDeploymentStatus` each mislead on their own: STABLE does not mean
RUNNING, and READY says nothing about the job. The tool derives one value, FAILED, DEGRADED, STUCK, SUSPENDED or OK,
from all three plus drift, `status.error`, TaskManager counts and pod states. The derivation is a pure function with
table tests over every combination found in captured fixtures, and `ls`, filters, sorting, `why` and the TUI all use
it.

Time-based rules are relative to the operator's effective reconcile interval, which the profile can set and which
defaults to the operator's 60 seconds. `status.error` under STABLE counts only after one interval for FlinkDeployment,
because the operator clears it on the next observe, but immediately for FlinkSessionJob, where it persists. Thresholds
are never stricter than the operator's own.
