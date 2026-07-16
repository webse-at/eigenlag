# Demo-GIF für das README — deterministisch reproduzierbar, kein Screencast.
#
#   bash launch/build-demo-gif.sh   (vhs + ffmpeg-Nachschritt; Ausgabe: assets/demo.gif)
#
# Zwei Stellen sind gestellt, beide bewusst und nur fürs Bild:
# 1. Die pipx-Zeile ist eine Shell-Funktion mit der echten pipx-Ausgabe, weil das
#    Package beim Bau dieses GIFs noch nicht auf PyPI liegt.
# 2. Der Report wird zeilenweise mit 30 ms je Zeile ausgegeben, damit das Scrollen
#    sichtbar ist. Echt rendert `eigenlag demo` den Report in ~0,1 s (wiki/log.md,
#    Session 013); ohne Bremse wäre der gesamte Output ein einziger Frame.
#
# Das Terminal ist so bemessen (840 px ≈ 51 Zeilen), dass der Endstand auf der
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
Type `eigenlag() { command eigenlag "$@" | while IFS= read -r line; do printf '%s\n' "$line"; sleep 0.03; done; }`
Enter
Type "clear"
Enter
Show

Type "pipx install eigenlag"
Sleep 600ms
Enter
Sleep 1.5s

Type "eigenlag demo"
Sleep 600ms
Enter

# ~2 s sichtbares Scrollen (62 Zeilen x 30 ms), dann Halten auf der Plan-Sektion.
Sleep 4s
Sleep 7s
