# 9. Platform specifics in a profile

The core assumes nothing about labels, naming, connectors or environments. A profile at
`~/.config/kubectl-flink/config.yaml`, selected by context, carries scopes, the production matcher (context pattern,
namespace pattern or namespace label), write policies, known owners and their commands, selector aliases, columns,
thresholds, extra verdict rules, a stateful-job rule and extra config sources for `report`.

The file has an `apiVersion` (`kubectl-flink/v1alpha1`) and a list of profiles, each with `match`, a list of
context name patterns; the first match is used. `KUBECTL_FLINK_CONFIG` and `XDG_CONFIG_HOME` move the file.
`--context` narrows the profile's scopes to that context, `-n` replaces their namespaces and `-A` widens them.

Defaults work without a profile. `config view` shows the effective profile and `config validate` checks it against a
versioned schema. A platform team ships its profile next to its own tooling, outside this repository.
