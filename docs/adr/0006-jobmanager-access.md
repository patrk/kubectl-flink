# 6. JobManager access through an in-process port-forward

JobManager REST is reached by port-forwarding to the JobManager pod from inside the process, with client-go, on a
random local port. Nothing needs a separate `kubectl port-forward`, an ingress or a reachable service IP, and the REST
service may be headless. The pool keeps the four most recently used forwards open and runs one request set at a time,
cancelled when the user moves on.

The client reads the fields common to Flink 1.20 and 2.x: `exceptionHistory` rather than the removed top-level
`root-exception`, the aggregated subtask metrics endpoint, and optional source metrics such as `pendingRecords`. A
`--rest-url` flag bypasses the forward for users who already have an endpoint.
