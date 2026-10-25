Output docs/demo/ls.gif
Require kubectl
Set Height 900
Source docs/demo/setup.tape

Sleep 600ms
Type "# every Flink job in the cluster, one health value each" Sleep 700ms Enter
Type "kubectl flink ls -A" Sleep 500ms Enter
Sleep 4.5s
Type "# only what needs attention" Sleep 700ms Enter
Type "kubectl flink ls -A --unhealthy" Sleep 500ms Enter
Sleep 5s
