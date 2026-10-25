# A short portrait overview, readable on a phone: about 67 columns in 4:5, so the TUI's narrow layout is what is
# shown. MP4 for players that autoplay video muted; the typed comments carry the story without sound. Needs
# make demo flood: the example jobs among 2,000 more.
Output docs/demo/out/highlights.mp4
Output docs/demo/out/highlights.gif
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
Type "# 2,009 FlinkDeployments in 44 namespaces: 9 run" Sleep 300ms Enter
Type "# here, 2,000 carry statuses copied from real jobs" Sleep 900ms Enter
Type "# Which ones need you, and why?" Sleep 900ms Enter
Type "kubectl flink top -A" Sleep 500ms Enter
Sleep 4s
# Every job, the worst first; then one job's verdict.
Sleep 1s
Type@120ms "/order-export" Sleep 500ms Enter Sleep 700ms
Enter Sleep 5s
Escape Sleep 600ms
# A running job's operators, coloured as the Flink web UI colours them.
Escape Sleep 300ms
Type@120ms "/settlement" Sleep 500ms Enter Sleep 700ms
Enter Sleep 2.5s
Type "2" Sleep 4.5s
Escape Sleep 400ms
Type "q" Sleep 500ms
Ctrl+L Sleep 400ms
Type "# kubectl flink: one command per question," Sleep 300ms Enter
Type "# for 9 jobs or 2,000" Sleep 900ms Enter
Type "# github.com/patrk/kubectl-flink" Sleep 3s
