"""Shared IMAP plumbing used by imap_delete.py, imap_gui_app/, and folder_list.py.

Kept dependency-free (stdlib only) so every script in this repo stays that way.
"""
import imaplib
import re
import ssl
import threading


def create_ssl_context(min_version=ssl.TLSVersion.TLSv1_2):
    ctx = ssl.create_default_context()
    ctx.minimum_version = min_version
    return ctx


def imap_login(server, user, password, timeout=None):
    """Connect and log in; does not select a mailbox."""
    mail = imaplib.IMAP4_SSL(server, ssl_context=create_ssl_context(), timeout=timeout)
    mail.login(user, password)
    return mail


def connect_and_select(server, user, password, mailbox, timeout):
    mail = imap_login(server, user, password, timeout=timeout)

    status, _ = mail.select(f'"{mailbox}"')
    if status != "OK":
        raise RuntimeError(f"Cannot select mailbox '{mailbox}'.")

    mail.sock.settimeout(timeout)
    return mail


def find_trash_folder(mail) -> str:
    """Locate the server's Trash mailbox name.

    Prefers the RFC 6154 \\Trash SPECIAL-USE flag; falls back to a
    case-insensitive name heuristic. Works with anything exposing a
    mail.list() -> (status, [bytes, ...]) IMAP4-shaped interface.
    """
    status, boxes = mail.list()
    if status != "OK":
        raise RuntimeError("Unable to list mailboxes.")

    def _extract_name(decoded: str) -> str:
        if '"' in decoded:
            return decoded.split('"')[-2]
        return decoded.split()[-1]

    name_match = None
    for raw in boxes:
        decoded = raw.decode("utf-8", errors="replace")
        flags_match = re.match(r"\(([^)]*)\)", decoded)
        if flags_match:
            flags = flags_match.group(1).lower()
            if r"\trash" in flags:
                return _extract_name(decoded)
        if name_match is None and "trash" in decoded.lower():
            name_match = _extract_name(decoded)

    if name_match:
        return name_match
    raise RuntimeError("Trash folder not found.")


def compress_uids(uid_list) -> bytes:
    """Collapse a list of UIDs into an IMAP range set, e.g. b'1:3,7,9:11'."""
    if not uid_list:
        return b""
    ints = sorted(int(u) for u in uid_list)
    ranges = []
    start = end = ints[0]

    for n in ints[1:]:
        if n == end + 1:
            end = n
        else:
            ranges.append(f"{start}:{end}" if start != end else str(start))
            start = end = n

    ranges.append(f"{start}:{end}" if start != end else str(start))
    return ",".join(ranges).encode()


def exponential_backoff(attempt, base_delay):
    return base_delay * (2 ** attempt)


def workers_for_server(server: str, default: int) -> int:
    s = server.lower()
    if "gmail" in s:
        return 10
    if "yahoo" in s:
        return 3
    return default


class ThreadConnectionPool:
    """Tracks one IMAP connection per worker thread, reconnectable on demand.

    `get(connect_fn)` returns the calling thread's connection, creating one
    via `connect_fn()` if it doesn't have one yet (or its connection was
    already evicted by close_all()). `replace(conn)` swaps in a freshly
    reconnected connection for the calling thread after a failure.
    close_all() logs out and forgets every tracked connection; safe to call
    from a different thread (e.g. a signal handler).
    """

    def __init__(self):
        self._local = threading.local()
        self._active = set()
        self._lock = threading.Lock()

    def get(self, connect_fn):
        conn = getattr(self._local, "conn", None)
        with self._lock:
            valid = conn is not None and conn in self._active
        if not valid:
            conn = connect_fn()
            self.replace(conn)
        return conn

    def replace(self, conn):
        self._local.conn = conn
        with self._lock:
            self._active.add(conn)

    def close_all(self):
        with self._lock:
            conns = list(self._active)
            self._active.clear()
        for conn in conns:
            try:
                conn.logout()
            except Exception:
                pass
