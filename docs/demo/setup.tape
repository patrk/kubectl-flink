# Shared by the demo tapes: the plugin on PATH and a private kubeconfig whose current context is the demo cluster,
# so that the commands read as users type them. Run from the repository root after make build demo. Each tape
# sets its height before sourcing this: VHS applies settings only before the first other command.
Set Shell "bash"
Set FontSize 14
Set Width 1720
Set Padding 16
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms
Hide
Type `export PATH="$PWD/bin:$PATH" KUBECONFIG="$(mktemp)" PS1='$ '` Enter
Type `kubectl config view --minify --flatten --context kind-kubectl-flink --kubeconfig ~/.kube/config > "$KUBECONFIG"` Enter
Type "clear" Enter
Show
