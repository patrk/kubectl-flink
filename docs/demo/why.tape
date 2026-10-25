Output docs/demo/why.gif
Require kubectl
Set Height 1420
Source docs/demo/look.tape
Source docs/demo/env.tape

Sleep 600ms
# Jobs are found by the ID people use for them, here a use-case label.
Type "# why is use case 5101 restarting?" Sleep 700ms Enter
Type "kubectl flink why -A 5101" Sleep 500ms Enter
Sleep 9s
