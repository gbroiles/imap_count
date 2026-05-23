#!/usr/bin/env python3
"""IMAP Sender Manager GUI.

Reads connection details from imap.ini (server, username, password, folder,
optional ignore list), fetches all messages in the chosen folder (up to
10,000 for yahoo.com), groups them by sender, and lets the user check off
senders whose messages should be moved to the Trash folder.

Runs on Windows and macOS (and Linux); uses only the Python standard library.
"""
import configparser
import concurrent.futures
import imaplib
import logging
import os
import queue
import re
import socket
import ssl
import sys
import threading
import time
import tkinter as tk
from collections import Counter
from email import message_from_bytes
from email.utils import parseaddr
from pathlib import Path
from tkinter import messagebox, ttk

imaplib._MAXLINE = 100_000_000

CONNECTION_TIMEOUT = 60
FETCH_CHUNK_SIZE = 1000
DELETE_CHUNK_SIZE = 100
YAHOO_MESSAGE_CAP = 10_000

INI_FILENAME = "imap.ini"
INI_SECTION = "imap"
INI_KEYS = ("server", "username", "password", "folder", "ignore")
REQUIRED_INI_KEYS = ("server", "username", "password", "folder")

logging.basicConfig(
    filename="imap_gui.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
)
log = logging.getLogger(__name__)


# ---------- Resilient IMAP wrapper (adapted from imap_count.py) ----------


class ResilientIMAP:
    def __init__(self, host, user, password, timeout=CONNECTION_TIMEOUT, retries=3):
        self.host = host
        self.user = user
        self.password = password
        self.timeout = timeout
        self.retries = retries
        self.mail = None
        self.current_folder = None
        self.readonly = False
        self._connect()

    def _connect(self):
        if self.mail:
            try:
                self.mail.logout()
            except Exception:
                pass
        ssl_context = ssl.create_default_context()
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        self.mail = imaplib.IMAP4_SSL(self.host, ssl_context=ssl_context, timeout=self.timeout)
        self.mail.login(self.user, self.password)
        log.info("Connected to %s as %s", self.host, self.user)
        if self.current_folder is not None:
            self.mail.select(self.current_folder, readonly=self.readonly)

    def select(self, folder, readonly=False):
        self.current_folder = folder
        self.readonly = readonly
        # Strip any stray quotes the user may have typed before re-quoting.
        safe = folder.strip('"')
        return self.mail.select(f'"{safe}"', readonly=readonly)

    def _retry(self, op_name, *args, **kwargs):
        last_exc = RuntimeError(f"Operation '{op_name}' failed after retries")
        for attempt in range(self.retries):
            try:
                func = getattr(self.mail, op_name)
                return func(*args, **kwargs)
            except (imaplib.IMAP4.abort, socket.error, EOFError, ssl.SSLError) as exc:
                last_exc = exc
                log.warning("%s failed (%s); retry %d/%d", op_name, exc, attempt + 1, self.retries)
                if attempt < self.retries - 1:
                    time.sleep(2 * (attempt + 1))
                    try:
                        self._connect()
                    except Exception as reconnect_exc:
                        last_exc = reconnect_exc
        raise last_exc

    def uid(self, *args, **kwargs):
        return self._retry("uid", *args, **kwargs)

    def list(self, *args, **kwargs):
        return self._retry("list", *args, **kwargs)

    def expunge(self):
        return self._retry("expunge")

    def capabilities(self):
        return self.mail.capabilities

    def logout(self):
        try:
            self.mail.close()
        except Exception:
            pass
        try:
            self.mail.logout()
        except Exception:
            pass


# ---------- Helpers ----------


def find_trash_folder(mail: ResilientIMAP) -> str:
    """Locate the server's Trash mailbox name.

    Prefers the RFC 6154 \\Trash SPECIAL-USE flag; falls back to a
    case-insensitive name heuristic.
    """
    status, boxes = mail.list()
    if status != "OK":
        raise RuntimeError("Unable to list mailboxes")

    def _extract_name(decoded: str) -> str:
        """Return the folder name from a decoded LIST response line."""
        if '"' in decoded:
            return decoded.split('"')[-2]
        return decoded.split()[-1]

    name_match = None
    for raw in boxes:
        decoded = raw.decode("utf-8", errors="replace")
        # Flags live inside the first set of parentheses, e.g. (\HasNoChildren \Trash).
        flags_match = re.match(r"\(([^)]*)\)", decoded)
        if flags_match:
            flags = flags_match.group(1).lower()
            if r"\trash" in flags:
                return _extract_name(decoded)
        if name_match is None and "trash" in decoded.lower():
            name_match = _extract_name(decoded)

    if name_match:
        return name_match
    raise RuntimeError("Trash folder not found on server")


def compress_uids(uid_list):
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


def workers_for_server(server: str) -> int:
    s = server.lower()
    if "gmail" in s:
        return 10
    if "yahoo" in s:
        return 3
    return 5


# ---------- INI file ----------


def ini_paths():
    here = Path(__file__).resolve().parent / INI_FILENAME
    cwd = Path.cwd() / INI_FILENAME
    # Preserve order, remove duplicates.
    seen = set()
    paths = []
    for p in (here, cwd):
        if p not in seen:
            seen.add(p)
            paths.append(p)
    return paths


def load_ini():
    """Return (config_dict, path_used, error_message_or_None).

    config_dict has keys server/username/password/folder/ignore (ignore may
    be ''). If no INI exists, returns ({}, None, None). If an INI exists but
    is malformed or incomplete, returns (partial_dict, path, error).
    """
    for path in ini_paths():
        if path.is_file():
            parser = configparser.ConfigParser()
            try:
                parser.read(path, encoding="utf-8")
            except configparser.Error as exc:
                return {}, path, f"Could not parse {path}: {exc}"
            if INI_SECTION not in parser:
                return {}, path, f"{path} is missing the [{INI_SECTION}] section"
            section = parser[INI_SECTION]
            data = {k: section.get(k, "").strip() for k in INI_KEYS}
            missing = [k for k in REQUIRED_INI_KEYS if not data.get(k)]
            if missing:
                err = f"{path} is missing required key(s): {', '.join(missing)}"
                return data, path, err
            return data, path, None
    return {}, None, None


def save_ini(data: dict, path: Path) -> None:
    parser = configparser.ConfigParser()
    parser[INI_SECTION] = {k: data.get(k, "") for k in INI_KEYS}
    with open(path, "w") as fh:
        parser.write(fh)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


# ---------- Ignore list ----------


def parse_ignore(raw: str):
    """Split a comma/whitespace list of entries; return list of lowercase rules.

    Each rule is either an exact email (contains '@') or a domain.
    Domain rules may start with '@' (e.g. '@example.com') or be a bare
    domain ('example.com'); both match any address whose domain part
    equals (or is a subdomain of) the rule.
    """
    if not raw:
        return []
    parts = re.split(r"[\s,]+", raw.strip())
    return [p.lower() for p in parts if p]


def make_ignore_predicate(rules):
    if not rules:
        return lambda email: False
    exact = set()
    domains = []
    for rule in rules:
        rule = rule.lower()
        if "@" in rule and not rule.startswith("@"):
            # Looks like a full email.
            exact.add(rule)
        else:
            domain = rule.lstrip("@")
            if domain:
                domains.append(domain)

    def predicate(email_addr: str) -> bool:
        e = email_addr.lower()
        if e in exact:
            return True
        if "@" in e:
            dom = e.split("@", 1)[1]
            for d in domains:
                if dom == d or dom.endswith("." + d):
                    return True
        return False

    return predicate


# ---------- Worker thread-local connection management ----------


_thread_local = threading.local()
_open_connections: set = set()  # set allows O(1) membership check
_open_connections_lock = threading.Lock()


def _get_thread_connection(server, user, password, folder, readonly):
    conn = getattr(_thread_local, "conn", None)
    # Re-create if absent or if the connection was closed by a previous operation
    # (a recycled thread from an old executor will have a stale, logged-out conn
    # that has already been removed from _open_connections).
    with _open_connections_lock:
        valid = conn is not None and conn in _open_connections
    if not valid:
        conn = ResilientIMAP(server, user, password)
        conn.select(folder, readonly=readonly)
        _thread_local.conn = conn
        with _open_connections_lock:
            _open_connections.add(conn)
    return conn


def _close_all_thread_connections():
    with _open_connections_lock:
        conns = list(_open_connections)
        _open_connections.clear()
    for conn in conns:
        try:
            conn.logout()
        except Exception:
            pass


# ---------- Fetch / Delete worker functions ----------


def fetch_headers_chunk(uid_chunk, server, user, password, folder, cancel_flag):
    if cancel_flag.is_set():
        return []
    conn = _get_thread_connection(server, user, password, folder, readonly=True)
    fetch_arg = compress_uids(uid_chunk)
    results = []
    try:
        status, data = conn.uid("FETCH", fetch_arg, "(BODY.PEEK[HEADER.FIELDS (FROM)])")
        if status != "OK":
            log.warning("UID FETCH returned %s", status)
            return results
        # data is a list alternating tuples ((b'1 (UID 123 BODY[...] {n}', b'From: ...')) and b')'
        current_uid = None
        for part in data:
            if isinstance(part, tuple):
                envelope, body = part
                m = re.search(rb"UID (\d+)", envelope)
                if m:
                    current_uid = m.group(1).decode()
                try:
                    msg = message_from_bytes(body)
                    name, addr = parseaddr(msg.get("From", "") or "")
                    if current_uid and addr:
                        results.append((current_uid, name.strip(), addr.strip().lower()))
                except Exception as exc:
                    log.debug("Header parse failed: %s", exc)
    except Exception as exc:
        log.error("fetch_headers_chunk error: %s", exc)
    return results


def delete_uids_chunk(uid_chunk, trash_folder, supports_move, server, user, password, folder, cancel_flag):
    if cancel_flag.is_set():
        return 0
    conn = _get_thread_connection(server, user, password, folder, readonly=False)
    processed = 0
    chunk_size = DELETE_CHUNK_SIZE
    quoted_trash = f'"{trash_folder}"'
    i = 0
    retries = 5
    base_delay = 2.0
    while i < len(uid_chunk):
        if cancel_flag.is_set():
            break
        sub = uid_chunk[i : i + chunk_size]
        uid_str = compress_uids(sub)
        for attempt in range(retries):
            if cancel_flag.is_set():
                break
            try:
                if supports_move:
                    status, resp = conn.uid("MOVE", uid_str, quoted_trash)
                    if status != "OK":
                        raise RuntimeError(f"MOVE failed: {resp}")
                else:
                    status, resp = conn.uid("COPY", uid_str, quoted_trash)
                    if status != "OK":
                        raise RuntimeError(f"COPY failed: {resp}")
                    status, resp = conn.uid("STORE", uid_str, "+FLAGS", r"(\Deleted)")
                    if status != "OK":
                        raise RuntimeError(f"STORE failed: {resp}")
                i += len(sub)
                processed += len(sub)
                break
            except (imaplib.IMAP4.abort, ssl.SSLError, socket.error) as exc:
                delay = base_delay * (2 ** attempt)
                log.warning("Connection lost during delete (%s); sleeping %ss", exc, delay)
                time.sleep(delay)
            except Exception as exc:
                msg = str(exc).upper()
                if "LIMIT" in msg or "OVERQUOTA" in msg:
                    new_size = max(10, chunk_size // 2)
                    if new_size < chunk_size:
                        log.warning("Rate limit; shrinking chunk %d -> %d", chunk_size, new_size)
                        chunk_size = new_size
                    delay = 15.0 * (2 ** attempt)
                    log.warning("Rate limit, sleeping %ss", delay)
                    time.sleep(delay)
                else:
                    log.error("Delete chunk failed: %s", exc)
                    if attempt == retries - 1:
                        return processed
                    time.sleep(base_delay)
    return processed


# ---------- Background orchestrators ----------


def run_fetch(cfg, cancel_flag, event_q):
    """Fetch headers; emit progress / done / error events into event_q."""
    server = cfg["server"]
    user = cfg["username"]
    password = cfg["password"]
    folder = cfg["folder"]
    is_yahoo = "yahoo" in server.lower()
    workers = workers_for_server(server)

    try:
        main = ResilientIMAP(server, user, password)
        status, _ = main.select(folder, readonly=True)
        if status != "OK":
            event_q.put(("error", f"Cannot select folder '{folder}'"))
            main.logout()
            return
        status, data = main.uid("SEARCH", None, "ALL")
        if status != "OK":
            event_q.put(("error", "UID SEARCH failed"))
            main.logout()
            return
        uids = data[0].split() if data and data[0] else []
        main.logout()
    except Exception as exc:
        log.exception("Fetch init failed")
        event_q.put(("error", f"Connection failed: {exc}"))
        return

    if is_yahoo and len(uids) > YAHOO_MESSAGE_CAP:
        log.info("Yahoo cap: trimming %d -> %d UIDs", len(uids), YAHOO_MESSAGE_CAP)
        uids = uids[-YAHOO_MESSAGE_CAP:]

    total = len(uids)
    event_q.put(("status", f"Fetching headers for {total} messages..."))
    event_q.put(("progress_init", total))
    if total == 0:
        event_q.put(("fetch_done", ([], 0)))
        return

    chunks = [uids[i : i + FETCH_CHUNK_SIZE] for i in range(0, total, FETCH_CHUNK_SIZE)]
    all_records = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(fetch_headers_chunk, c, server, user, password, folder, cancel_flag): c
                for c in chunks
            }
            for fut in concurrent.futures.as_completed(futures):
                if cancel_flag.is_set():
                    break
                try:
                    recs = fut.result()
                    all_records.extend(recs)
                except Exception as exc:
                    log.error("Fetch chunk failed: %s", exc)
                event_q.put(("progress", len(futures[fut])))
    finally:
        _close_all_thread_connections()

    event_q.put(("fetch_done", (all_records, total)))


def run_delete(cfg, uids_to_delete, cancel_flag, event_q):
    server = cfg["server"]
    user = cfg["username"]
    password = cfg["password"]
    folder = cfg["folder"]
    workers = workers_for_server(server)

    try:
        main = ResilientIMAP(server, user, password)
        status, _ = main.select(folder, readonly=False)
        if status != "OK":
            event_q.put(("error", f"Cannot select folder '{folder}' for delete"))
            main.logout()
            return
        trash = find_trash_folder(main)
        supports_move = b"MOVE" in main.capabilities()
    except Exception as exc:
        log.exception("Delete init failed")
        event_q.put(("error", f"Delete init failed: {exc}"))
        return

    total = len(uids_to_delete)
    event_q.put(("status", f"Moving {total} messages to {trash}..."))
    event_q.put(("progress_init", total))

    per_worker = max(1, total // workers)
    chunks = [uids_to_delete[i : i + per_worker] for i in range(0, total, per_worker)]
    moved = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(delete_uids_chunk, c, trash, supports_move, server, user, password, folder, cancel_flag)
                for c in chunks
            ]
            for fut in concurrent.futures.as_completed(futures):
                if cancel_flag.is_set():
                    break
                try:
                    n = fut.result()
                    moved += n
                    event_q.put(("progress", n))
                except Exception as exc:
                    log.error("Delete chunk crashed: %s", exc)
    finally:
        if not supports_move and not cancel_flag.is_set():
            try:
                main.expunge()
            except Exception as exc:
                log.warning("Expunge failed: %s", exc)
        _close_all_thread_connections()
        try:
            main.logout()
        except Exception:
            pass

    event_q.put(("delete_done", moved))


# ---------- Config-entry dialog ----------


class ConfigDialog(tk.Toplevel):
    """Modal dialog used when imap.ini is missing or incomplete."""

    def __init__(self, master, initial=None, save_path=None):
        super().__init__(master)
        self.title("IMAP configuration")
        self.resizable(False, False)
        self.transient(master)
        self.result = None  # dict on OK/Save; None on Cancel.
        self.saved = False
        self._save_path = save_path or (Path(__file__).resolve().parent / INI_FILENAME)
        initial = initial or {}

        self._vars = {
            "server": tk.StringVar(value=initial.get("server", "")),
            "username": tk.StringVar(value=initial.get("username", "")),
            "password": tk.StringVar(value=initial.get("password", "")),
            "folder": tk.StringVar(value=initial.get("folder", "INBOX") or "INBOX"),
            "ignore": tk.StringVar(value=initial.get("ignore", "")),
        }

        frm = ttk.Frame(self, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")

        rows = [
            ("Server", "server", False),
            ("Username", "username", False),
            ("Password", "password", True),
            ("Folder", "folder", False),
            ("Ignore (comma- or space-separated)", "ignore", False),
        ]
        for i, (label, key, secret) in enumerate(rows):
            ttk.Label(frm, text=label).grid(row=i, column=0, sticky="e", padx=(0, 8), pady=4)
            entry = ttk.Entry(frm, textvariable=self._vars[key], width=40,
                              show="•" if secret else "")
            entry.grid(row=i, column=1, sticky="we", pady=4)

        btns = ttk.Frame(frm)
        btns.grid(row=len(rows), column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(btns, text="Cancel", command=self._on_cancel).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Save", command=self._on_save).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="OK", command=self._on_ok).pack(side="right")

        self.bind("<Return>", lambda _e: self._on_ok())
        self.bind("<Escape>", lambda _e: self._on_cancel())
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        self.update_idletasks()
        self._center_on_parent()
        self.grab_set()

    def _center_on_parent(self):
        master = self.master
        if master and master.winfo_viewable():
            x = master.winfo_rootx() + (master.winfo_width() - self.winfo_width()) // 2
            y = master.winfo_rooty() + (master.winfo_height() - self.winfo_height()) // 2
            self.geometry(f"+{max(x, 0)}+{max(y, 0)}")

    def _collect(self):
        data = {k: v.get().strip() for k, v in self._vars.items()}
        missing = [k for k in REQUIRED_INI_KEYS if not data.get(k)]
        if missing:
            messagebox.showerror(
                "Missing fields",
                "Please fill in: " + ", ".join(missing),
                parent=self,
            )
            return None
        return data

    def _on_ok(self):
        data = self._collect()
        if data is None:
            return
        self.result = data
        self.saved = False
        self.destroy()

    def _on_save(self):
        data = self._collect()
        if data is None:
            return
        if self._save_path.exists():
            if not messagebox.askyesno(
                "Overwrite?",
                f"{self._save_path} already exists. Overwrite?",
                parent=self,
            ):
                return
        try:
            save_ini(data, self._save_path)
        except OSError as exc:
            messagebox.showerror("Save failed", str(exc), parent=self)
            return
        self.result = data
        self.saved = True
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()


# ---------- Main window ----------


CHECK_OFF = "☐"  # ☐
CHECK_ON = "☑"  # ☑


class SenderManagerApp:
    def __init__(self, root: tk.Tk, cfg: dict):
        self.root = root
        self.cfg = cfg
        self.ignore_predicate = make_ignore_predicate(parse_ignore(cfg.get("ignore", "")))

        # Sender data: email -> dict(count, name, uids:list[str])
        self.senders = {}
        # Set of checked sender emails.
        self.checked = set()
        # Maps Treeview row iid -> sender email.
        self.row_email = {}
        # Operation plumbing.
        self.cancel_flag = threading.Event()
        self.event_q: queue.Queue = queue.Queue()
        self.op_in_progress = False
        self._progress_max = 1
        self._progress_val = 0

        self.root.title("IMAP Sender Manager")
        self.root.geometry("760x560")
        self.root.minsize(560, 380)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._apply_theme()
        self._build_ui()
        # Start an initial fetch as soon as the window is up.
        self.root.after(150, self.refresh)
        self.root.after(100, self._drain_events)

    # -- UI construction --

    def _apply_theme(self):
        try:
            style = ttk.Style()
            if sys.platform != "darwin":
                # Prefer 'clam' for consistent look on Windows/Linux.
                themes = style.theme_names()
                if "clam" in themes:
                    style.theme_use("clam")
        except tk.TclError:
            pass

    def _build_ui(self):
        # Top bar.
        top = ttk.Frame(self.root, padding=(10, 8))
        top.pack(side="top", fill="x")
        ttk.Label(top, text="Folder:").pack(side="left")
        ttk.Label(top, text=self.cfg["folder"], font=("TkDefaultFont", 10, "bold")).pack(
            side="left", padx=(4, 12)
        )
        ttk.Label(top, text="Server:").pack(side="left")
        ttk.Label(top, text=self.cfg["server"]).pack(side="left", padx=(4, 12))
        self.refresh_btn = ttk.Button(top, text="Refresh", command=self.refresh)
        self.refresh_btn.pack(side="right")

        # Select-all bar.
        sa_bar = ttk.Frame(self.root, padding=(10, 0))
        sa_bar.pack(side="top", fill="x")
        self.select_all_var = tk.BooleanVar(value=False)
        self.select_all_cb = ttk.Checkbutton(
            sa_bar,
            text="Select all",
            variable=self.select_all_var,
            command=self._toggle_select_all,
        )
        self.select_all_cb.pack(side="left", pady=4)

        # Treeview.
        mid = ttk.Frame(self.root, padding=(10, 4))
        mid.pack(side="top", fill="both", expand=True)
        columns = ("check", "count", "sender")
        self.tree = ttk.Treeview(mid, columns=columns, show="headings", selectmode="none")
        self.tree.heading("check", text="", command=self._toggle_select_all_from_header)
        self.tree.heading("count", text="Count")
        self.tree.heading("sender", text="Sender")
        self.tree.column("check", width=36, anchor="center", stretch=False)
        self.tree.column("count", width=80, anchor="e", stretch=False)
        self.tree.column("sender", width=520, anchor="w", stretch=True)
        vsb = ttk.Scrollbar(mid, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self.tree.bind("<Button-1>", self._on_tree_click)

        # Bottom bar.
        bot = ttk.Frame(self.root, padding=(10, 8))
        bot.pack(side="bottom", fill="x")
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(bot, textvariable=self.status_var).pack(side="left")
        self.delete_btn = ttk.Button(bot, text="Delete selected (0)", command=self._on_delete, state="disabled")
        self.delete_btn.pack(side="right")
        self.progress = ttk.Progressbar(bot, mode="determinate", length=180)
        self.progress.pack(side="right", padx=(0, 10))

    # -- Sender list rendering --

    def _populate_tree(self):
        self.tree.delete(*self.tree.get_children())
        self.row_email.clear()
        # Sort descending by count then by email.
        items = sorted(
            self.senders.items(),
            key=lambda kv: (-kv[1]["count"], kv[0]),
        )
        for email_addr, info in items:
            check_glyph = CHECK_ON if email_addr in self.checked else CHECK_OFF
            display = email_addr if not info["name"] else f"{info['name']} <{email_addr}>"
            iid = self.tree.insert("", "end", values=(check_glyph, info["count"], display))
            self.row_email[iid] = email_addr
        self._update_delete_button()
        self._sync_select_all_var()

    def _on_tree_click(self, event):
        if self.op_in_progress:
            return
        region = self.tree.identify("region", event.x, event.y)
        if region == "heading":
            col = self.tree.identify_column(event.x)
            if col == "#1":
                self._toggle_select_all_from_header()
            return
        if region != "cell":
            return
        row = self.tree.identify_row(event.y)
        if not row:
            return
        col = self.tree.identify_column(event.x)
        if col != "#1":
            return
        email_addr = self.row_email.get(row)
        if email_addr is None:
            return
        if email_addr in self.checked:
            self.checked.discard(email_addr)
            glyph = CHECK_OFF
        else:
            self.checked.add(email_addr)
            glyph = CHECK_ON
        values = list(self.tree.item(row, "values"))
        values[0] = glyph
        self.tree.item(row, values=values)
        self._update_delete_button()
        self._sync_select_all_var()

    def _toggle_select_all_from_header(self):
        # Header click: invert select-all.
        self.select_all_var.set(not self.select_all_var.get())
        self._toggle_select_all()

    def _toggle_select_all(self):
        if self.op_in_progress:
            return
        on = self.select_all_var.get()
        if on:
            self.checked = set(self.senders.keys())
        else:
            self.checked.clear()
        glyph = CHECK_ON if on else CHECK_OFF
        for iid, email_addr in self.row_email.items():
            values = list(self.tree.item(iid, "values"))
            values[0] = glyph
            self.tree.item(iid, values=values)
        self._update_delete_button()

    def _sync_select_all_var(self):
        all_checked = bool(self.senders) and len(self.checked) == len(self.senders)
        # Use trace-free assignment by temporarily swapping; BooleanVar has no trace here.
        self.select_all_var.set(all_checked)

    def _update_delete_button(self):
        n_msgs = sum(self.senders[e]["count"] for e in self.checked if e in self.senders)
        self.delete_btn.configure(text=f"Delete selected ({n_msgs})")
        if self.op_in_progress or n_msgs == 0:
            self.delete_btn.state(["disabled"])
        else:
            self.delete_btn.state(["!disabled"])

    # -- Operation lifecycle --

    def _set_op_in_progress(self, on: bool):
        self.op_in_progress = on
        state = "disabled" if on else "!disabled"
        self.refresh_btn.state([state])
        if on:
            self.select_all_cb.state(["disabled"])
            self.delete_btn.state(["disabled"])
        else:
            self.select_all_cb.state(["!disabled"])
            self._update_delete_button()

    def refresh(self):
        if self.op_in_progress:
            return
        self.cancel_flag.clear()
        self._set_op_in_progress(True)
        self.progress.configure(value=0, maximum=1)
        self.status_var.set("Connecting...")
        threading.Thread(
            target=run_fetch,
            name="FetchOrchestrator",
            args=(self.cfg, self.cancel_flag, self.event_q),
            daemon=True,
        ).start()

    def _on_delete(self):
        if self.op_in_progress or not self.checked:
            return
        n_msgs = sum(self.senders[e]["count"] for e in self.checked if e in self.senders)
        n_senders = len(self.checked)
        ok = messagebox.askyesno(
            "Confirm delete",
            f"Move {n_msgs} message(s) from {n_senders} sender(s) to Trash?",
            parent=self.root,
        )
        if not ok:
            return
        uids = []
        for email_addr in self.checked:
            info = self.senders.get(email_addr)
            if info:
                uids.extend(info["uids"])
        if not uids:
            return
        self.cancel_flag.clear()
        self._set_op_in_progress(True)
        self.progress.configure(value=0, maximum=max(1, len(uids)))
        self.status_var.set(f"Moving {len(uids)} message(s) to Trash...")
        threading.Thread(
            target=run_delete,
            name="DeleteOrchestrator",
            args=(self.cfg, uids, self.cancel_flag, self.event_q),
            daemon=True,
        ).start()

    # -- Event pump from worker threads --

    def _drain_events(self):
        try:
            while True:
                kind, payload = self.event_q.get_nowait()
                self._handle_event(kind, payload)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._drain_events)

    def _handle_event(self, kind, payload):
        if kind == "status":
            self.status_var.set(payload)
        elif kind == "progress_init":
            self._progress_max = max(1, payload)
            self._progress_val = 0
            self.progress.configure(value=0, maximum=self._progress_max)
        elif kind == "progress":
            self._progress_val = min(self._progress_val + payload, self._progress_max)
            self.progress.configure(value=self._progress_val)
        elif kind == "fetch_done":
            records, total = payload
            self._on_fetch_done(records, total)
        elif kind == "delete_done":
            moved = payload
            self.status_var.set(f"Moved {moved} message(s) to Trash. Refreshing...")
            self._set_op_in_progress(False)
            self.checked.clear()
            self.root.after(100, self.refresh)
        elif kind == "error":
            self._set_op_in_progress(False)
            self.status_var.set("Error.")
            messagebox.showerror("IMAP error", str(payload), parent=self.root)

    def _on_fetch_done(self, records, total):
        # Aggregate records: (uid, name, email).
        counter = Counter()
        names = {}
        uids_by_sender = {}
        for uid, name, addr in records:
            if self.ignore_predicate(addr):
                continue
            counter[addr] += 1
            uids_by_sender.setdefault(addr, []).append(uid)
            if name and addr not in names:
                names[addr] = name
        self.senders = {
            addr: {"count": cnt, "name": names.get(addr, ""), "uids": uids_by_sender[addr]}
            for addr, cnt in counter.items()
        }
        self.checked.clear()
        self._populate_tree()
        kept = sum(info["count"] for info in self.senders.values())
        ignored = total - kept
        self.status_var.set(
            f"{total} message(s) scanned, {len(self.senders)} sender(s) shown"
            + (f" ({ignored} ignored)" if ignored else "")
            + "."
        )
        self._set_op_in_progress(False)

    def _on_close(self):
        self.cancel_flag.set()
        # Give workers a moment to notice; they're daemons so we don't block.
        self.root.after(150, self.root.destroy)


# ---------- Entry point ----------


def _enable_high_dpi_on_windows():
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            pass


def _prompt_for_config(root: tk.Tk, initial=None):
    """Open the modal config dialog; return (cfg, saved) or (None, False)."""
    save_path = Path(__file__).resolve().parent / INI_FILENAME
    dlg = ConfigDialog(root, initial=initial, save_path=save_path)
    root.wait_window(dlg)
    if dlg.result is None:
        return None, False
    return dlg.result, dlg.saved


def main():
    _enable_high_dpi_on_windows()
    root = tk.Tk()
    root.withdraw()

    cfg, path, err = load_ini()
    if err:
        # File exists but is malformed: show the error then prompt with whatever parsed.
        messagebox.showerror("Config error", err, parent=root)
        cfg, _saved = _prompt_for_config(root, initial=cfg)
        if cfg is None:
            root.destroy()
            return
    elif not cfg:
        # No INI at all: prompt the user.
        cfg, _saved = _prompt_for_config(root)
        if cfg is None:
            root.destroy()
            return

    log.info(
        "Starting GUI: server=%s user=%s folder=%s ignore_rules=%s",
        cfg.get("server"), cfg.get("username"), cfg.get("folder"),
        parse_ignore(cfg.get("ignore", "")),
    )

    root.deiconify()
    SenderManagerApp(root, cfg)
    root.mainloop()


if __name__ == "__main__":
    main()
