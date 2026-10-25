Output docs/demo/classic.gif
Require kubectl
Set Height 1240
Source docs/demo/look.tape
Source docs/demo/env.tape

Sleep 600ms
Type "# the classic way: which Flink jobs are broken?" Sleep 700ms Enter
Type "kubectl get flinkdeployments -A" Sleep 500ms Enter
Sleep 3s
Type "# no health column; dig into one that looks odd" Sleep 700ms Enter
Type "kubectl describe flinkdeployment order-export -n orders | grep -A2 'Error:'" Sleep 500ms Enter
Sleep 3.5s
Type "kubectl logs -n orders deploy/order-export --previous --tail 8" Sleep 500ms Enter
Sleep 4s
Type "# ...and repeat for every job, namespace and cluster" Sleep 1.5s Enter
Sleep 1s
Ctrl+L Sleep 500ms
Type "# with kubectl flink" Sleep 700ms Enter
Type "kubectl flink ls -A --unhealthy" Sleep 500ms Enter
Sleep 3.5s
Type "kubectl flink why orders/order-export" Sleep 500ms Enter
Sleep 8s
