Output docs/demo/out/ids.gif
Require kubectl
Set Height 820
Source docs/demo/look.tape
Source docs/demo/env.tape

Sleep 600ms
Type "# people name jobs by the IDs they work with, not by resource names" Sleep 800ms Enter
Type "kubectl flink ls -A 4711" Sleep 500ms Enter
Sleep 3s
Type "kubectl flink ls -A -l team=orders" Sleep 500ms Enter
Sleep 3.5s
Type "kubectl flink why -A 5102" Sleep 500ms Enter
Sleep 7s
