#!/usr/bin/env python3
"""IMAP Sender Manager GUI.

Reads connection details from imap.ini (server, username, password, folder,
optional ignore list), fetches all messages in the chosen folder (up to
10,000 for yahoo.com), groups them by sender, and lets the user check off
senders whose messages should be moved to the Trash folder.

Runs on Windows and macOS (and Linux); uses only the Python standard library.
Implementation lives in imap_gui_app/ (config, resilient connection wrapper,
background worker orchestration, and the Tkinter UI).
"""
from imap_gui_app.ui import main

if __name__ == "__main__":
    main()
