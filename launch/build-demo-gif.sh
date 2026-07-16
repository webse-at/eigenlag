#!/usr/bin/env bash
# Baut assets/demo.gif reproduzierbar aus launch/demo.tape.
#
# Zwei Schritte, weil vhs 0.11.0 `Set Framerate` fuer GIF-Output ignoriert
# (gemessen: 25 fps trotz Framerate 10) und 25 fps beim Zeilen-Scrollen die
# 3-MB-Grenze sprengen: erst vhs (Rohfassung), dann ffmpeg auf 10 fps mit
# neu gerechneter Palette (dither=none haelt Text scharf und das GIF klein).
#
# Braucht: vhs, ttyd (~/.local/bin), ffmpeg. Chromium-Sandbox am Server nicht
# nutzbar, daher VHS_NO_SANDBOX.
set -euo pipefail
cd "$(dirname "$0")/.."

VHS_NO_SANDBOX=true PATH="$HOME/.local/bin:$PATH" vhs launch/demo.tape
ffmpeg -y -i assets/demo.gif -filter_complex \
  "[0:v]fps=10,split[a][b];[a]palettegen=stats_mode=diff[p];[b][p]paletteuse=dither=none:diff_mode=rectangle" \
  assets/demo-opt.gif
mv assets/demo-opt.gif assets/demo.gif
ls -la assets/demo.gif
