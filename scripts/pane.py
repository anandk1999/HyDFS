#!/usr/bin/env python3
import asyncio
import iterm2
import os

# Set your full script path here:
MY_SCRIPT = "/Users/ASUS1/Desktop/Fall 2025 Semester (UIUC)/Distributed Systems/Machine Problems/MP2/mp3-g02/scripts/vm.sh"
ROWS = 5
COLS = 2

async def main(connection):
    app = await iterm2.async_get_app(connection)

    # Use the current terminal window (safer & more compatible).
    window = app.current_terminal_window
    if window is None:
        raise RuntimeError("No iTerm terminal window found. Please open an iTerm terminal window and try again.")

    tab = window.current_tab
    # Top-left (existing) session
    left_prev = tab.current_session
    sessions = []
    sessions.append(left_prev)

    # create top-right by splitting left_prev vertically (left/right)
    right_prev = await left_prev.async_split_pane(vertical=True)
    # wait briefly for UI to settle
    await asyncio.sleep(0.12)
    sessions.append(right_prev)

    # create remaining rows by splitting the previous left/right horizontally (top->bottom)
    for _ in range(2, ROWS + 1):
        new_left = await left_prev.async_split_pane(vertical=False)   # horizontal split (creates below)
        await asyncio.sleep(0.12)
        sessions.append(new_left)
        left_prev = new_left

        new_right = await right_prev.async_split_pane(vertical=False)
        await asyncio.sleep(0.12)
        sessions.append(new_right)
        right_prev = new_right

    # Send one command per session (row-major). Quote the script path to preserve spaces.
    quoted = "'" + MY_SCRIPT + "'"
    for i, s in enumerate(sessions, start=1):
        cmd = f"{quoted} {i}\n"
        await s.async_send_text(cmd)
        # small delay to avoid racing
        await asyncio.sleep(0.08)

if __name__ == "__main__":
    # Run with iterm2 helper to avoid permission/connection problems:
    # /Applications/iTerm.app/Contents/Resources/it2run pane.py
    asyncio.run(iterm2.run_until_complete(main))
