# 7. Evidence first, verdict rules as data

`why` cannot know every way a Flink job fails, so it is built around the evidence and names only the patterns it
knows. It collects, in parallel and within 20 s, the operator status, the pods, the Warning events of the resource
and its pods (selected by uid, because events of an earlier resource with the same name linger), the error lines of
the JobManager log and of its previous container after a crash, and, when the JobManager is ready, its REST API:
exception history, checkpoints and their configuration. What cannot be collected is listed with the reason.

The evidence is reduced to signals: a fixed vocabulary of kinds (`event`, `pod.waiting`, `log.jobmanager`,
`exception.cause`, `checkpoint.stale` and others) with their text. Rules in YAML match signals with regular
expressions, take named captures into a title, an explanation and a next step, and may supersede the rules for the
symptoms of their cause, as an image that cannot be pulled supersedes the JobManager crash loop. The verdict lists
the matching rules by severity, then by layer: Kubernetes, operator, runtime. A job no rule explains gets the
strongest generic signals instead, such as the deepest exception cause. Health stays what `ls` shows and runs on
timers; a rule's severity says how bad the pattern is, even within the grace period.

Default rules are embedded in the binary and stay generic. A profile adds rules in the same format, replaces a
default by reusing its id, or disables one. Each default rule ships with a captured scenario that triggers it, or,
for failures kind cannot induce cheaply, a synthetic signal case; a test enforces this. Rules are validated when
loaded: known signal kinds and layers, compiling expressions, unique ids, and templates that use only captures
their conditions define.
