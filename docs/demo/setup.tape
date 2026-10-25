# Shared by the demo tapes: the look, the plugin on PATH, and a private kubeconfig whose current context is the demo
# cluster, so that commands read as users type them. Run from the repository root after make build demo. Each tape
# sets its height before sourcing this: VHS applies settings only before the first other command.
Set Shell "bash"
Set FontFamily "MesloLGS NF"
Set FontSize 16
Set LineHeight 1.2
Set Width 1960
Set Padding 28
Set Margin 36
Set MarginFill "#5B4BC4"
Set WindowBar Colorful
Set WindowBarSize 40
Set BorderRadius 14
Set Theme "Catppuccin Mocha"
Set TypingSpeed 55ms
Set CursorBlink false
Set Framerate 30
Hide
Type `export PATH="$PWD/bin:$PATH" KUBECONFIG="$(mktemp)" PS1='\[\e[38;5;183m\]❯\[\e[0m\] '` Enter
Type `kubectl config view --minify --flatten --context kind-kubectl-flink --kubeconfig ~/.kube/config > "$KUBECONFIG"` Enter
Type "clear" Enter
Show
