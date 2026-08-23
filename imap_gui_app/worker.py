"""Background fetch/delete orchestration used by the Tkinter UI."""
import concurrent.futures
import imaplib
import logging
import re
import socket
import ssl
import time
from email import message_from_bytes
from email.utils import parseaddr

from imap_common import ThreadConnectionPool, compress_uids, exponential_backoff, find_trash_folder, workers_for_server
from .resilient import ResilientIMAP

FETCH_CHUNK_SIZE = 1000
DELETE_CHUNK_SIZE = 100
YAHOO_MESSAGE_CAP = 10_000

log = logging.getLogger(__name__)

_pool = ThreadConnectionPool()


def _get_thread_connection(server, user, password, folder, readonly):
    def connect():
        conn = ResilientIMAP(server, user, password)
        conn.select(folder, readonly=readonly)
        return conn

    return _pool.get(connect)


def _close_all_thread_connections():
    _pool.close_all()


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
                delay = exponential_backoff(attempt, base_delay)
                log.warning("Connection lost during delete (%s); sleeping %ss", exc, delay)
                time.sleep(delay)
            except Exception as exc:
                msg = str(exc).upper()
                if "LIMIT" in msg or "OVERQUOTA" in msg:
                    new_size = max(10, chunk_size // 2)
                    if new_size < chunk_size:
                        log.warning("Rate limit; shrinking chunk %d -> %d", chunk_size, new_size)
                        chunk_size = new_size
                    delay = exponential_backoff(attempt, 15.0)
                    log.warning("Rate limit, sleeping %ss", delay)
                    time.sleep(delay)
                else:
                    log.error("Delete chunk failed: %s", exc)
                    if attempt == retries - 1:
                        return processed
                    time.sleep(base_delay)
    return processed


def run_fetch(cfg, cancel_flag, event_q):
    """Fetch headers; emit progress / done / error events into event_q."""
    server = cfg["server"]
    user = cfg["username"]
    password = cfg["password"]
    folder = cfg["folder"]
    is_yahoo = "yahoo" in server.lower()
    workers = workers_for_server(server, default=5)

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
    workers = workers_for_server(server, default=5)

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
