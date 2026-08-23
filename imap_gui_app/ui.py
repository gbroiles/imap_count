"""Tkinter widgets and app wiring for the IMAP Sender Manager GUI."""
import logging
import queue
import sys
import threading
import tkinter as tk
from collections import Counter
from pathlib import Path
from tkinter import messagebox, ttk

from .config import (
    REQUIRED_INI_KEYS,
    load_ini,
    make_ignore_predicate,
    parse_ignore,
    save_ini,
)
from .worker import run_delete, run_fetch

log = logging.getLogger(__name__)

CHECK_OFF = "☐"  # ☐
CHECK_ON = "☑"  # ☑


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
        self._save_path = save_path or (Path(__file__).resolve().parent.parent / "imap.ini")
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
    save_path = Path(__file__).resolve().parent.parent / "imap.ini"
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
