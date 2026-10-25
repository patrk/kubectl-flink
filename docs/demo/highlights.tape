# A short portrait overview, readable on a phone: about 67 columns in 4:5, so the TUI's narrow layout is what is
# shown. MP4 for players that autoplay video muted; the typed comments carry the story without sound.
Output docs/demo/highlights.mp4
Output docs/demo/highlights.gif
Require kubectl
Set FontFamily "MesloLGS NF"
Set FontSize 24
Set LineHeight 1.2
Set Width 1080
Set Height 1350
Set Padding 24
Set Margin 32
Set MarginFill "#0d1117"
Set WindowBar Colorful
Set WindowBarSize 44
Set BorderRadius 16
Set Theme "Catppuccin Mocha"
Set TypingSpeed 50ms
Set CursorBlink false
Set Framerate 30
Source docs/demo/env.tape

Sleep 700ms
Type "# 9 Flink jobs on Kubernetes." Sleep 600ms Enter
Type "# Which are broken, and why?" Sleep 900ms Enter
Type "kubectl flink top -A" Sleep 500ms Enter
Sleep 3.5s
# The unhealthy only; the worst first, its verdict.
Type "f" Sleep 2s
Enter Sleep 5s
Escape Sleep 600ms
# A healthy job's operators, coloured as the Flink web UI colours them.
Escape Sleep 300ms
Type@120ms "/settlement" Sleep 500ms Enter Sleep 700ms
Enter Sleep 2.5s
Type "2" Sleep 4.5s
Escape Sleep 400ms
Type "q" Sleep 500ms
Ctrl+L Sleep 400ms
Type "# kubectl flink: one command per question," Sleep 300ms Enter
Type "# every job, every cluster" Sleep 900ms Enter
Type "# github.com/patrk/kubectl-flink" Sleep 3s
