Output docs/demo/out/graph.gif
Require kubectl
Require jq
Set Height 900
Source docs/demo/look.tape
Source docs/demo/env.tape

Sleep 600ms
Type "# where does the time of a job go? as the Flink web UI shows it, without opening it" Sleep 800ms Enter
Type "kubectl flink graph payments/settlement" Sleep 500ms Enter
Sleep 5s
Type "# the same as data, for scripts" Sleep 700ms Enter
Type "kubectl flink graph payments/settlement -o json | jq -c '.operators[] | {name, state, busyMsPerSecond, recordsOutPerSecond}'" Sleep 500ms Enter
Sleep 6s
