# The plugin on PATH and a private kubeconfig whose current context is the demo cluster, so that commands read as
# users type them. Run from the repository root after make build demo. A tape sets every setting before sourcing
# this: VHS applies settings only before the first other command.
Set Shell "bash"
Hide
Type `export PATH="$PWD/bin:$PATH" KUBECONFIG="$(mktemp)" PS1='\[\e[38;5;183m\]❯\[\e[0m\] '` Enter
Type `kubectl config view --minify --flatten --context kind-kubectl-flink --kubeconfig ~/.kube/config > "$KUBECONFIG"` Enter
Type "clear" Enter
Show
