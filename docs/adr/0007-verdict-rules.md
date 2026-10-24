# 7. Verdict rules as data

`why` collects signals from four layers plus HA state: exceptions, events, pod states, status fields, metrics. Rules in
YAML map signals to findings with a layer, a severity, a plain-language explanation and an optional remedy. Default
rules ship embedded in the binary and cover operator states, OOM, image pulls, scheduling, admission denials, missing
mounts, connection failures and HA leftovers.

A profile can add rules, for example a log signature specific to one platform with the command that fixes it. The
verdict orders findings as a causal chain and prints it before the evidence. Rules are tested against captured
fixtures, so a new rule ships with the fixture that triggers it.
