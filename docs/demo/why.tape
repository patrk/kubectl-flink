Output docs/demo/why.gif
Require kubectl
Set Height 820
Source docs/demo/setup.tape

# Jobs are found by the ID people use for them, here a use-case label.
Type "kubectl flink why -A 5101" Sleep 400ms Enter
Sleep 7s
