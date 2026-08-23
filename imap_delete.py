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

from imap_common import (
    ThreadConnectionPool,
    compress_uids,
    connect_and_select,
    exponential_backoff,
    find_trash_folder,
    workers_for_server,
)

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
pool = ThreadConnectionPool()

def signal_handler(sig, frame):
    logger.warning("Interrupt received. Gracefully shutting down connections...")
    print("\nInterrupt received. Halting and cleaning up...")
    shutdown_flag.set()
    pool.close_all()
    sys.exit(0)

# ---------- IMAP Helpers ----------

EMAIL_REGEX = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'

def validate_sender(sender: str) -> bool:
    return bool(re.fullmatch(EMAIL_REGEX, sender))

def get_imap_date_before(days: int) -> str:
    target_date = datetime.now() - timedelta(days=days)
    return target_date.strftime("%d-%b-%Y")

def get_thread_connection(server, args):
    return pool.get(
        lambda: connect_and_select(server, args.user, args.password, args.folder, args.timeout)
    )

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
    logger.debug(f"Running IMAP SEARCH: {query}")
    status, data = mail.uid("search", None, query)
    if status != "OK" or not data or not data[0]:
        return []
    return data[0].split()

def run_gmail_search(mail, raw_query):
    logger.debug(f"Running GMAIL RAW SEARCH: {raw_query}")
    status, data = mail.uid("search", "X-GM-RAW", f'"{raw_query}"')
    if status != "OK" or not data or not data[0]:
        return []
    return data[0].split()

# ---------- Worker Thread ----------

def process_chunk(chunk_uids, trash_folder, supports_move, args, server, pbar):
    if shutdown_flag.is_set():
        return

    current_chunk_size = args.chunk_size
    i = 0

    mail = get_thread_connection(server, args)
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
                    logger.debug(f"Executing MOVE for {len(sub_chunk)} items. Attempt {attempt + 1}")
                    status, response = mail.uid("MOVE", uid_str, quoted_trash)
                    if status != "OK":
                        raise RuntimeError(f"MOVE failed: {response}")
                else:
                    logger.debug(f"Executing COPY for {len(sub_chunk)} items. Attempt {attempt + 1}")
                    status, response = mail.uid("COPY", uid_str, quoted_trash)
                    if status != "OK":
                        raise RuntimeError(f"COPY failed: {response}")

                    logger.debug(f"Executing STORE (Delete Flag) for {len(sub_chunk)} items.")
                    status, response = mail.uid("STORE", uid_str, "+FLAGS", r"\Deleted")
                    if status != "OK":
                        raise RuntimeError(f"STORE failed: {response}")

                i += len(sub_chunk)
                pbar.update(len(sub_chunk))

                if args.chunk_delay > 0:
                    time.sleep(args.chunk_delay)
                break

            except (imaplib.IMAP4.abort, ssl.SSLError, socket.error) as e:
                delay = exponential_backoff(attempt, args.delay)
                logger.warning(f"Connection lost ({e}). Retrying in {delay}s.")
                wait_with_progress(delay, f"Reconnecting ({delay}s)")

                try:
                    mail = connect_and_select(server, args.user, args.password, args.folder, args.timeout)
                    pool.replace(mail)
                except Exception as reconnect_e:
                    logger.error(f"Reconnection failed: {reconnect_e}")

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
                        return
                else:
                    if attempt == args.retries - 1:
                        logger.error(f"Failed to process chunk: {error_msg}")
                        return
                    wait_with_progress(args.delay, f"Retrying ({args.delay}s)")

def expunge_with_retry(main_mail, server, args):
    for attempt in range(args.retries):
        if shutdown_flag.is_set():
            return main_mail

        try:
            main_mail.expunge()
            return main_mail
        except (imaplib.IMAP4.abort, ssl.SSLError, socket.error):
            delay = exponential_backoff(attempt, args.delay)
            logger.warning(f"Connection lost before expunge. Retrying in {delay}s.")
            wait_with_progress(delay, f"Reconnecting ({delay}s)")

            if shutdown_flag.is_set():
                return main_mail

            try:
                main_mail = connect_and_select(
                    server, args.user, args.password, args.folder, args.timeout
                )
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")

    logger.error("Failed to expunge deleted messages after exhausting retries.")
    return main_mail

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
    parser.add_argument("--chunk-size", type=int, default=None, help="Number of emails to move per request")
    parser.add_argument("--chunk-delay", type=float, default=1.0, help="Seconds to wait between chunks to avoid rate limits")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debugging output")

    args = parser.parse_args()

    # Apply verbose logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled.")

    if not args.user or not args.password:
        sys.exit("Error: Username and password must be provided via command-line arguments or environment variables.")

    server = args.server.lower()
    is_gmail = "gmail" in server

    # Apply defaults if missing
    if args.threads is None:
        args.threads = workers_for_server(server, default=1)

    if args.chunk_size is None:
        args.chunk_size = 12 if "yahoo" in server else 100

    logger.debug(f"Configuration: Threads={args.threads}, Chunk Size={args.chunk_size}, Delay={args.chunk_delay}s")

    # Main connection for initial setup
    main_mail = connect_and_select(server, args.user, args.password, args.folder, args.timeout)
    trash_folder = find_trash_folder(main_mail)
    supports_move = b"MOVE" in main_mail.capabilities
    logger.debug(f"Trash folder mapped to: {trash_folder}. Server supports MOVE: {supports_move}")

    if is_gmail:
        search_query = build_gmail_raw_query(args)
    else:
        search_query = build_standard_search(args)

    pass_number = 1

    while not shutdown_flag.is_set():
        try:
            if is_gmail:
                uids = run_gmail_search(main_mail, search_query)
            else:
                uids = run_standard_search(main_mail, search_query)
        except (imaplib.IMAP4.abort, ssl.SSLError, socket.error) as e:
            logger.warning(f"Main connection lost during search ({e}). Reconnecting...")
            try:
                main_mail = connect_and_select(server, args.user, args.password, args.folder, args.timeout)
                continue
            except Exception as reconnect_e:
                logger.error(f"Failed to reconnect main mail instance: {reconnect_e}")
                break
        except Exception as e:
            logger.error(f"Search failed: {e}")
            break

        total = len(uids)

        if total == 0:
            if pass_number == 1:
                logger.info("No matching messages.")
            else:
                logger.info(f"Pass {pass_number}: 0 messages found. Processing complete.")
            break

        if args.dry_run:
            logger.info(f"[DRY RUN] {total} messages would be moved.")
            break

        logger.info(f"Pass {pass_number}: Processing {total} messages using {args.threads} threads...")

        # Split total UIDs evenly among threads
        chunk_size = max(1, len(uids) // args.threads)
        chunks = [uids[i:i + chunk_size] for i in range(0, len(uids), chunk_size)]

        with tqdm(total=total, unit="msg", desc=f"Pass {pass_number}") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
                futures = {
                    executor.submit(process_chunk, chunk, trash_folder, supports_move, args, server, pbar): chunk
                    for chunk in chunks
                }

                for future in concurrent.futures.as_completed(futures):
                    if shutdown_flag.is_set():
                        break
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Thread execution failed: {e}")

        # Expunge on the main thread after all workers finish the current pass
        if not supports_move and not shutdown_flag.is_set():
            logger.info("Expunging deleted messages to finalize pass...")
            main_mail = expunge_with_retry(main_mail, server, args)

        pass_number += 1

    # Cleanup all connections
    pool.close_all()

    try:
        main_mail.logout()
    except Exception:
        pass
    logger.info("Completed.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    move_to_trash()
