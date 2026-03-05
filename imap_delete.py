#!/usr/bin/env python3
import imaplib
import os
import re
import ssl
import sys
import logging
import time
import argparse
import socket
import threading
import concurrent.futures
import signal
from datetime import datetime, timedelta
from tqdm import tqdm

imaplib._MAXLINE = 10000000

LOG_FILE = "imap_errors.log"

# ---------- Logging & Signals ----------

def secure_file_handler(path: str):
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    fd = os.open(path, flags, 0o600)
    os.close(fd)

    handler = logging.FileHandler(path)
    handler.setLevel(logging.WARNING)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s - %(message)s")
    handler.setFormatter(formatter)
    return handler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(secure_file_handler(LOG_FILE))
logger.addHandler(logging.StreamHandler(sys.stdout))

shutdown_flag = threading.Event()
active_connections = []
connection_lock = threading.Lock()
thread_local = threading.local()

def signal_handler(sig, frame):
    logger.warning("Interrupt received. Gracefully shutting down connections...")
    print("\nInterrupt received. Halting and cleaning up...")
    shutdown_flag.set()
    
    with connection_lock:
        for conn in active_connections:
            try:
                conn.logout()
            except Exception:
                pass
    sys.exit(0)

# ---------- IMAP Helpers ----------

EMAIL_REGEX = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'

def validate_sender(sender: str) -> bool:
    return bool(re.fullmatch(EMAIL_REGEX, sender))

def get_imap_date_before(days: int) -> str:
    target_date = datetime.now() - timedelta(days=days)
    return target_date.strftime("%d-%b-%Y")

def connect_and_select(server, user, password, mailbox, timeout):
    ssl_context = ssl.create_default_context()
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

    mail = imaplib.IMAP4_SSL(server, ssl_context=ssl_context, timeout=timeout)
    mail.login(user, password)

    status, _ = mail.select(f'"{mailbox}"')
    if status != "OK":
        raise RuntimeError(f"Cannot select mailbox '{mailbox}'.")

    mail.sock.settimeout(timeout)
    return mail

def get_thread_connection(server, user, password, folder, timeout):
    if not hasattr(thread_local, "mail"):
        logger.info("Initializing new thread-local IMAP connection.")
        conn = connect_and_select(server, user, password, folder, timeout)
        thread_local.mail = conn
        with connection_lock:
            active_connections.append(conn)
    return thread_local.mail

def find_trash_folder(mail):
    status, boxes = mail.list()
    if status != "OK":
        raise RuntimeError("Unable to list mailboxes.")

    for box in boxes:
        decoded = box.decode()
        if "trash" in decoded.lower():
            return decoded.split(' "/" ')[-1].strip('"')

    raise RuntimeError("Trash folder not found.")

def compress_uids(uid_list):
    if not uid_list:
        return b""
    ints = [int(u) for u in uid_list]
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

def wait_with_progress(delay_seconds: float, desc: str = "Waiting"):
    steps = int(delay_seconds)
    remainder = delay_seconds - steps
    
    if steps > 0:
        for _ in tqdm(range(steps), desc=desc, leave=False, unit="s"):
            if shutdown_flag.is_set(): return
            time.sleep(1)
            
    if remainder > 0 and not shutdown_flag.is_set():
        time.sleep(remainder)

# ---------- Search Builders ----------

def build_standard_search(args):
    if args.time is not None:
        cutoff_date = get_imap_date_before(args.time)
        return f'(SENTBEFORE {cutoff_date})'

    senders = []
    if args.sender:
        if not validate_sender(args.sender):
            sys.exit("Invalid sender format.")
        senders.append(args.sender)

    if args.file:
        with open(args.file, "r") as f:
            for line in f:
                s = line.strip()
                if s and validate_sender(s):
                    senders.append(s)

    if not senders:
        sys.exit("Provide sender, --file, or --time.")

    if len(senders) == 1:
        return f'(FROM "{senders[0]}")'

    query = f'(FROM "{senders[-1]}")'
    for sender in reversed(senders[:-1]):
        query = f'(OR (FROM "{sender}") {query})'
    return query

def build_gmail_raw_query(args):
    parts = []
    if args.time is not None:
        parts.append(f"older_than:{args.time}d")

    senders = []
    if args.sender:
        if not validate_sender(args.sender):
            sys.exit("Invalid sender format.")
        senders.append(args.sender)

    if args.file:
        with open(args.file, "r") as f:
            for line in f:
                s = line.strip()
                if s and validate_sender(s):
                    senders.append(s)

    if senders:
        sender_query = " OR ".join([f"from:{s}" for s in senders])
        parts.append(f"({sender_query})")

    if not parts:
        sys.exit("Provide sender, --file, or --time.")

    return " ".join(parts)

def run_standard_search(mail, query):
    status, data = mail.uid("search", None, query)
    if status != "OK" or not data or not data[0]:
        return []
    return data[0].split()

def run_gmail_search(mail, raw_query):
    status, data = mail.uid("search", "X-GM-RAW", f'"{raw_query}"')
    if status != "OK" or not data or not data[0]:
        return []
    return data[0].split()

# ---------- Worker Thread ----------

def process_chunk(chunk_uids, trash_folder, supports_move, args, server):
    if shutdown_flag.is_set():
        return 0

    processed = 0
    current_chunk_size = args.chunk_size
    i = 0
    
    mail = get_thread_connection(server, args.user, args.password, args.folder, args.timeout)
    quoted_trash = f'"{trash_folder}"'

    while i < len(chunk_uids):
        if shutdown_flag.is_set():
            break

        sub_chunk = chunk_uids[i:i + current_chunk_size]
        uid_str = compress_uids(sub_chunk)

        for attempt in range(args.retries):
            if shutdown_flag.is_set():
                break

            try:
                if supports_move:
                    status, response = mail.uid("MOVE", uid_str, quoted_trash)
                    if status != "OK":
                        raise RuntimeError(f"MOVE failed: {response}")
                else:
                    status, response = mail.uid("COPY", uid_str, quoted_trash)
                    if status != "OK":
                        raise RuntimeError(f"COPY failed: {response}")

                    status, response = mail.uid("STORE", uid_str, "+FLAGS", r"\Deleted")
                    if status != "OK":
                        raise RuntimeError(f"STORE failed: {response}")

                i += len(sub_chunk)
                processed += len(sub_chunk)
                
                if args.chunk_delay > 0:
                    time.sleep(args.chunk_delay)
                break

            except (imaplib.IMAP4.abort, ssl.SSLError, socket.error):
                delay = exponential_backoff(attempt, args.delay)
                logger.warning(f"Connection lost. Retrying in {delay}s.")
                wait_with_progress(delay, f"Reconnecting ({delay}s)")
                
                try:
                    mail = connect_and_select(server, args.user, args.password, args.folder, args.timeout)
                    thread_local.mail = mail
                    # Update active connections list safely
                    with connection_lock:
                        if mail not in active_connections:
                            active_connections.append(mail)
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}")

            except Exception as e:
                error_msg = str(e)
                if "LIMIT" in error_msg.upper():
                    delay = exponential_backoff(attempt, 15.0) 
                    
                    new_size = max(10, current_chunk_size // 2)
                    if new_size < current_chunk_size:
                        logger.warning(f"Rate limit hit. Reducing chunk size from {current_chunk_size} to {new_size}.")
                        current_chunk_size = new_size
                        
                    logger.warning(f"Pausing for {delay}s before retry.")
                    wait_with_progress(delay, f"Rate limit ({delay}s)")
                    
                    if attempt == args.retries - 1:
                        logger.error(f"Exhausted retries due to rate limits: {error_msg}")
                        return processed 
                else:
                    if attempt == args.retries - 1:
                        logger.error(f"Failed to process chunk: {error_msg}")
                        return processed
                    wait_with_progress(args.delay, f"Retrying ({args.delay}s)")

    return processed

# ---------- Main ----------

def move_to_trash():
    parser = argparse.ArgumentParser(description="Move emails to trash based on sender or time.")
    parser.add_argument("folder", help="Target IMAP folder to scan (e.g., INBOX)")
    parser.add_argument("sender", nargs="?", help="Specific sender email address")
    parser.add_argument("--file", help="File containing list of senders")
    parser.add_argument("--time", nargs='?', const=170, type=int, help="Delete emails older than X days")
    parser.add_argument("-u", "--user", default=os.getenv('GMAIL_ACCT'), help="IMAP username (defaults to GMAIL_ACCT env var)")
    parser.add_argument("-p", "--password", default=os.getenv('GMAIL_PASS'), help="IMAP password (defaults to GMAIL_PASS env var)")
    parser.add_argument("-s", "--server", default="imap.gmail.com", help="IMAP server (defaults to imap.gmail.com)")
    parser.add_argument("-t", "--threads", type=int, default=None, help="Number of concurrent threads")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=100, help="Number of emails to move per request")
    parser.add_argument("--chunk-delay", type=float, default=1.0, help="Seconds to wait between chunks to avoid rate limits")

    args = parser.parse_args()

    if not args.user or not args.password:
        sys.exit("Error: Username and password must be provided via command-line arguments or environment variables.")

    server = args.server.lower()
    is_gmail = "gmail" in server

    # Determine thread count
    if args.threads is not None:
        max_workers = args.threads
    elif "yahoo" in server:
        max_workers = 3
    elif "gmail" in server:
        max_workers = 10
    else:
        max_workers = 1

    # Main connection for initial setup
    main_mail = connect_and_select(server, args.user, args.password, args.folder, args.timeout)
    trash_folder = find_trash_folder(main_mail)
    supports_move = b"MOVE" in main_mail.capabilities

    if is_gmail:
        search_query = build_gmail_raw_query(args)
        uids = run_gmail_search(main_mail, search_query)
    else:
        search_query = build_standard_search(args)
        uids = run_standard_search(main_mail, search_query)

    total = len(uids)

    if total == 0:
        logger.info("No matching messages.")
        main_mail.logout()
        return

    if args.dry_run:
        logger.info(f"[DRY RUN] {total} messages would be moved.")
        main_mail.logout()
        return

    logger.info(f"Processing {total} messages using {max_workers} threads...")
    
    # Split total UIDs evenly among threads
    chunk_size = max(1, len(uids) // max_workers)
    chunks = [uids[i:i + chunk_size] for i in range(0, len(uids), chunk_size)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_chunk, chunk, trash_folder, supports_move, args, server): chunk 
            for chunk in chunks
        }
        
        with tqdm(total=total, unit="msg") as pbar:
            for future in concurrent.futures.as_completed(futures):
                if shutdown_flag.is_set():
                    break
                try:
                    processed_count = future.result()
                    pbar.update(processed_count)
                except Exception as e:
                    logger.error(f"Thread execution failed: {e}")

    # Expunge only on the main thread after all workers finish
    if not supports_move and not shutdown_flag.is_set():
        logger.info("Expunging deleted messages...")
        main_mail.expunge()

    # Cleanup all connections
    with connection_lock:
        for conn in active_connections:
            try:
                conn.logout()
            except Exception:
                pass
                
    main_mail.logout()
    logger.info("Completed.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    move_to_trash()
    

