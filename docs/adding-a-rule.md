# Turning an incident into a rule

`why` names failures it has a rule for and shows the evidence for everything else. When an incident needed a
human to recognise it, it is a candidate for a rule.

1. **Capture while it happens.** `kubectl flink report <job> --dir incident` keeps the evidence that disappears:
   events after an hour, the previous container's log after the next restart.
2. **Find the signal.** `kubectl flink why <job> -o json | jq .signals` lists what rules can match, by kind
   (`event`, `pod.waiting`, `log.jobmanager`, `exception.cause`, … in `internal/triage/signals.go`). Pick the
   text that names the cause, not a symptom: the exception's cause, not the restart it led to.
3. **Decide where the rule lives.**
   - A failure any Flink on Kubernetes can have goes into `internal/triage/rules.yaml`, in generic words.
   - A failure of your platform (its sidecars, its naming, its tooling) goes into your profile's `rules`, with the
     remedy your team uses. Nothing platform-specific goes into this repository.
4. **Write the rule.** Match the cause with a regular expression and take what the reader needs into named
   captures. A rule about a pod needs the pod in that state now (`pod.waiting`, `pod.unschedulable`, or
   `pod.mount` for a mount failure of a pod still starting); an event alone may tell of something that already
   resolved. Use `supersedes` for the symptoms the cause explains, and severity `OK` for a note about the past.
5. **Make it testable without the incident.**
   - If kind can reproduce it, add a scenario to `hack/capture-scenarios.sh` with a `check_why` line, and run
     `make scenarios`.
   - If not, add the signals to `synthetic` in `internal/triage/rules_test.go`. Write them from the incident with
     every name, ID, host and URL replaced by a neutral one.

   `TestEveryRuleFires` fails until one of the two exists.
6. **Check** `make test-committed lint`, and that `why` on the captured report still lists what the rule does not
   explain.
