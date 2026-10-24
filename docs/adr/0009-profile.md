# 9. Platform specifics in a profile

The core assumes nothing about labels, naming, connectors or environments. A profile at
`~/.config/kubectl-flink/config.yaml`, selected by context, carries scopes, the production matcher (context pattern,
namespace pattern or namespace label), write policies, known owners and their commands, selector aliases, columns,
thresholds, extra verdict rules, a stateful-job rule and extra config sources for `report`.

Defaults work without a profile. `config view` shows the effective profile and `config validate` checks it against a
versioned schema. A platform team ships its profile next to its own tooling, outside this repository.
