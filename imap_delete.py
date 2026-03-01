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
import json
from datetime import datetime, timedelta
from tqdm import tqdm

imaplib._MAXLINE = 10000000

LOG_FILE = "imap_errors.log"
CHECKPOINT_FILE = ".imap_move_checkpoint.json"
CHUNK_SIZE = 1500


# ---------- Logging ----------

def secure_file_handler(path: str):
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    fd = os.open(path, flags, 0o600)
    os.close(fd)

    handler = logging.FileHandler(path)
    handler.setLevel(logging.WARNING)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    return handler


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(secure_file_handler(LOG_FILE))
logger.addHandler(logging.StreamHandler(sys.stdout))


EMAIL_REGEX = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'


def validate_sender(sender: str) -> bool:
    return bool(re.fullmatch(EMAIL_REGEX, sender))


def get_imap_date_before(days: int) -> str:
    target_date = datetime.now() - timedelta(days=days)
    return target_date.strftime("%d-%b-%Y")


# ---------- IMAP Helpers ----------

def connect_and_select(server, user, password, mailbox, timeout):
    ssl_context = ssl.create_default_context()
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

    mail = imaplib.IMAP4_SSL(
        server,
        ssl_context=ssl_context,
        timeout=timeout,
    )

    mail.login(user, password)

    status, _ = mail.select(mailbox)
    if status != "OK":
        raise RuntimeError(f"Cannot select mailbox '{mailbox}'.")

    mail.sock.settimeout(timeout)
    return mail


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


# ---------- Checkpointing ----------

def save_checkpoint(index):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_index": index}, f)
    os.replace(tmp, CHECKPOINT_FILE)


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return 0
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
            return int(data.get("last_index", 0))
    except Exception:
        return 0


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


# ---------- Main ----------

def move_to_trash():
    parser = argparse.ArgumentParser()
    parser.add_argument("mailbox")
    parser.add_argument("sender", nargs="?")
    parser.add_argument("--file")
    parser.add_argument("--time", nargs='?', const=170, type=int)
    parser.add_argument("--server", default="imap.gmail.com")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    server = args.server.lower()
    is_gmail = "gmail" in server

    user = os.getenv("GMAIL_ACCT")
    password = os.getenv("GMAIL_PASS")
    if not user or not password:
        sys.exit("Missing credentials.")

    mail = connect_and_select(server, user, password, args.mailbox, args.timeout)
    trash_folder = find_trash_folder(mail)
    supports_move = b"MOVE" in mail.capabilities

    if is_gmail:
        search_query = build_gmail_raw_query(args)
        uids = run_gmail_search(mail, search_query)
    else:
        search_query = build_standard_search(args)
        uids = run_standard_search(mail, search_query)

    total = len(uids)

    if total == 0:
        logger.info("No matching messages.")
        return

    if args.dry_run:
        logger.info(f"[DRY RUN] {total} messages would be moved.")
        return

    start_index = load_checkpoint()
    logger.info(f"Processing {total} messages")

    with tqdm(total=total, initial=start_index, unit="msg") as pbar:
        i = start_index

        while i < total:
            chunk = uids[i:i + CHUNK_SIZE]
            uid_str = compress_uids(chunk)

            for attempt in range(args.retries):
                try:
                    if supports_move:
                        status, _ = mail.uid("MOVE", uid_str, trash_folder)
                        if status != "OK":
                            raise RuntimeError("MOVE failed")
                    else:
                        status, _ = mail.uid("COPY", uid_str, trash_folder)
                        if status != "OK":
                            raise RuntimeError("COPY failed")

                        status, _ = mail.uid(
                            "STORE", uid_str, "+FLAGS", r"\Deleted"
                        )
                        if status != "OK":
                            raise RuntimeError("STORE failed")

                    i += len(chunk)
                    save_checkpoint(i)
                    pbar.update(len(chunk))
                    break

                except (imaplib.IMAP4.abort,
                        ssl.SSLError,
                        socket.error):

                    delay = exponential_backoff(attempt, args.delay)
                    logger.warning(f"Connection lost. Retrying in {delay}s.")
                    time.sleep(delay)

                    mail = connect_and_select(
                        server, user, password,
                        args.mailbox,
                        args.timeout
                    )
                    trash_folder = find_trash_folder(mail)
                    supports_move = b"MOVE" in mail.capabilities

                    if is_gmail:
                        uids = run_gmail_search(mail, search_query)
                    else:
                        uids = run_standard_search(mail, search_query)

                    total = len(uids)

                except Exception:
                    if attempt == args.retries - 1:
                        raise
                    time.sleep(args.delay)

    if not supports_move:
        mail.expunge()

    clear_checkpoint()
    logger.info("Completed.")
    mail.logout()


if __name__ == "__main__":
    move_to_trash()

