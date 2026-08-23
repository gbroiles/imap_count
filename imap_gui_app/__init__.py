"""IMAP Sender Manager GUI implementation.

Entry point lives at the repo root in imap_gui.py; this package holds the
implementation split by concern: config (INI + ignore rules), resilient (the
retrying IMAP connection wrapper), worker (background fetch/delete
orchestration), and ui (Tkinter widgets and app wiring).
"""
import imaplib
import logging

imaplib._MAXLINE = 100_000_000

logging.basicConfig(
    filename="imap_gui.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
)
