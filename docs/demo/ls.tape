Output docs/demo/ls.gif
Require kubectl
Set Height 640
Source docs/demo/setup.tape

Type "kubectl flink ls -A" Sleep 400ms Enter
Sleep 4s
Type "kubectl flink ls -A --unhealthy" Sleep 400ms Enter
Sleep 5s
