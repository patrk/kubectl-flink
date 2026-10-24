# 6. JobManager access through an in-process port-forward

JobManager REST is reached by port-forwarding to the JobManager pod from inside the process, with client-go, on a
random local port. Nothing needs a separate `kubectl port-forward`, an ingress or a reachable service IP, and the REST
service may be headless. The pool keeps the four most recently used forwards open and runs one request set at a time,
cancelled when the user moves on.

The client reads the fields common to Flink 1.20 and 2.x: `exceptionHistory` rather than the removed top-level
`root-exception`, the aggregated subtask metrics endpoint, and optional source metrics such as `pendingRecords`.
Asking the aggregated endpoint for one metric a vertex does not have empties the whole answer, so optional
metrics, which carry an operator prefix (`<operator>.pendingRecords`), are requested only after the vertex has
listed them.

With several JobManagers under Kubernetes HA, every ready JobManager answers REST: a standby asks the leader
itself (checked on kind with Flink 2.2 and two JobManagers), so any ready pod will do. Only while a leader is being
elected does a JobManager answer 503; the pool then forwards to the next ready pod. Redirects are not followed,
because they would point at pod addresses that are not reachable from the user's machine. Port-forwarding
needs `create` on `pods/portforward`; without it the error names `--rest-url`. A
`--rest-url` flag bypasses the forward for users who already have an endpoint.
