Output docs/demo/out/crashloop.gif
Require kubectl
Set Height 1000
Source docs/demo/look.tape
Source docs/demo/env.tape

Sleep 600ms
Type "# a job keeps crashing: find it, see why, hand it over" Sleep 800ms Enter
Type "kubectl flink top -A" Sleep 500ms Enter
Sleep 3.5s
Type "f" Sleep 1.5s
Type@120ms "/order-export" Sleep 600ms Enter Sleep 800ms
Enter Sleep 5s
# The log from before the last crash: the current container has none yet.
Type "6" Sleep 4s
# The handover bundle, written while top stays open.
Type "R" Sleep 9s
Escape Sleep 500ms
Type "q" Sleep 800ms
Type "tar -xzOf kubectl-flink-report-*order-export*.tar.gz '*/summary.md' | head -32" Sleep 500ms Enter
Sleep 7s
Hide
Type "rm -f kubectl-flink-report-*order-export*.tar.gz" Enter
Show
