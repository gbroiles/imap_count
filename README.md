These are Python programs to manage and clean Gmail mailboxes from the command line.

`imap_gui.py` is a Tkinter GUI (Windows / macOS / Linux) that connects to any
IMAP server using settings from `imap.ini` (see `imap.ini.example`), lists
senders in the chosen folder sorted by message count, and lets you check off
senders to move their messages to Trash. If `imap.ini` is missing it pops up
a dialog to enter the settings, with an **OK** button to use them once and a
**Save** button to write them to `imap.ini`. Yahoo accounts are capped at the
10,000 most recent messages. Run with `python imap_gui.py` (no extra
dependencies — stdlib only).

