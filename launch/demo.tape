# Demo-GIF für das README — deterministisch reproduzierbar, kein Screencast.
#
#   vhs launch/demo.tape        (braucht vhs, ttyd, ffmpeg; Ausgabe: assets/demo.gif)
#
# Die pipx-Zeile ist gestellt (Shell-Funktion mit der echten pipx-Ausgabe), weil das
# Package beim Bau dieses GIFs noch nicht auf PyPI liegt. `eigenlag demo` läuft echt,
# über den pipx-Entry-Point. Das Terminal ist so bemessen, dass der Endstand auf der
# Plan-Sektion hält (Kante entfernt -> -43.18 %, "makes your current schedule sustainable").

Output assets/demo.gif

Set Shell "bash"
Set FontSize 13
Set Width 1200
Set Height 840
Set Padding 20
Set TypingSpeed 40ms

Hide
Type `export PATH="$HOME/.local/bin:$PATH"`
Enter
Type `pipx() { echo "  installed package eigenlag 0.1.0, installed using Python 3.14.4"; echo "  These apps are now globally available"; echo "    - eigenlag"; echo "done! ✨ 🌟 ✨"; }`
Enter
Type "clear"
Enter
Show

Type "pipx install eigenlag"
Sleep 800ms
Enter
Sleep 2s

Type "eigenlag demo"
Sleep 800ms
Enter
Sleep 3s

# Halten auf der Plan-Sektion: der -43-%-Moment.
Sleep 9s
