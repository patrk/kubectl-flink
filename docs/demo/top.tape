Output docs/demo/out/top.gif
Require kubectl
Set Height 900
Source docs/demo/look.tape
Source docs/demo/env.tape

Sleep 600ms
Type "kubectl flink top -A" Sleep 500ms Enter
Sleep 3.5s
# The unhealthy only, then the worst one's verdict, collected while the cursor rested on it.
Type "f" Sleep 2s
Enter Sleep 4.5s
Down@200ms 8 Sleep 1s
Escape Sleep 1s
# A job by name, and its job graph.
Escape Sleep 400ms
Type@120ms "/settlement" Sleep 700ms Enter Sleep 900ms
Enter Sleep 3s
Type "2" Sleep 5s
Escape Sleep 600ms
Type "q" Sleep 600ms
