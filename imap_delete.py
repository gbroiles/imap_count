 #!/usr/bin/env python3
import imaplib
import os
import re
import ssl
import sys
import stat
import logging
import time
import argparse
import socket
import json
from tqdm import tqdm

imaplib.Debug = 0

LOG_FILE = "imap_errors.log"
CHECKPOINT_FILE = ".imap_move_checkpoint.json"
CHUNK_SIZE = 100


# ---------- Secure Logging ----------

def secure_file_handler(path: str):
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    fd = os.open(path, flags, 0o600)
    os.close(fd)

    handler = logging.FileHandler(path)
    handler.setLevel(logging.WARNING)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    return handler


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(secure_file_handler(LOG_FILE))

console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(console)


# ---------- Email Validation ----------

EMAIL_REGEX = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'


def validate_sender(sender: str) -> bool:
    return bool(re.fullmatch(EMAIL_REGEX, sender))


# ---------- IMAP Helpers ----------

def connect_and_select(user, password, mailbox, timeout):
    ssl_context = ssl.create_default_context()
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

    mail = imaplib.IMAP4_SSL(
        "imap.gmail.com",
        ssl_context=ssl_context,
        timeout=timeout,
    )

    mail.login(user, password)

    status, _ = mail.select(mailbox)
    if status != "OK":
        raise RuntimeError(f"Cannot select mailbox '{mailbox}'.")

    mail.capability()
    return mail


def exponential_backoff(attempt, base_delay):
    return base_delay * (2 ** attempt)


def build_combined_from_query(senders):
    escaped = [s.replace('"', r'\"') for s in senders]

    if len(escaped) == 1:
        return f'(FROM "{escaped[0]}")'

    query = f'(FROM "{escaped[-1]}")'
    for sender in reversed(escaped[:-1]):
        query = f'(OR (FROM "{sender}") {query})'
    return query


# ---------- Checkpointing ----------

def save_checkpoint(index):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_index": index}, f)


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
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if not args.sender and not args.file:
        logger.error("Provide sender or --file.")
        sys.exit(1)

    if args.sender and args.file:
        logger.error("Provide only one of sender or --file.")
        sys.exit(1)

    if args.sender:
        if not validate_sender(args.sender):
            logger.error("Invalid sender format.")
            sys.exit(1)
        senders = [args.sender]
    else:
        with open(args.file, "r") as f:
            senders = [
                line.strip()
                for line in f
                if line.strip() and validate_sender(line.strip())
            ]

    user = os.getenv("GMAIL_ACCT")
    password = os.getenv("GMAIL_PASS")
    if not user or not password:
        logger.error("Missing credentials.")
        sys.exit(1)

    mail = connect_and_select(user, password, args.mailbox, args.timeout)
    supports_move = b"MOVE" in mail.capabilities

    search_query = build_combined_from_query(senders)

    status, data = mail.uid("search", None, search_query)
    if status != "OK" or not data or not data[0]:
        logger.info("No matching messages.")
        return

    uids = data[0].split()
    total = len(uids)

    if args.dry_run:
        logger.info(f"[DRY RUN] {total} messages would be moved.")
        return

    start_index = load_checkpoint()
    success_count = start_index

    logger.info(f"Processing {total} messages")

    with tqdm(total=total, initial=start_index, unit="msg") as pbar:
        i = start_index

        while i < total:
            chunk = uids[i:i + CHUNK_SIZE]
            uid_str = b",".join(chunk)

            for attempt in range(args.retries):
                try:
                    mail.noop()

                    if supports_move:
                        status, _ = mail.uid("MOVE", uid_str, "Trash")
                        if status != "OK":
                            raise RuntimeError("MOVE failed")
                    else:
                        c_status, _ = mail.uid("COPY", uid_str, "Trash")
                        if c_status != "OK":
                            raise RuntimeError("COPY failed")

                        s_status, _ = mail.uid(
                            "STORE", uid_str, "+FLAGS", r"\Deleted"
                        )
                        if s_status != "OK":
                            raise RuntimeError("STORE failed")

                    success_count += len(chunk)
                    i += len(chunk)
                    save_checkpoint(i)
                    pbar.update(len(chunk))
                    break

                except (imaplib.IMAP4.abort,
                        ssl.SSLError,
                        socket.error) as e:

                    delay = exponential_backoff(attempt, args.delay)
                    logger.warning(
                        f"Connection lost. Reconnecting in {delay}s..."
                    )
                    time.sleep(delay)

                    try:
                        mail = connect_and_select(
                            user, password,
                            args.mailbox,
                            args.timeout
                        )
                        supports_move = b"MOVE" in mail.capabilities
                    except Exception as reconnect_error:
                        if attempt == args.retries - 1:
                            logger.error(
                                f"Reconnect failed: {reconnect_error}"
                            )
                            raise
                        continue

                except Exception as e:
                    if attempt == args.retries - 1:
                        logger.error(f"Batch failed: {e}")
                        raise
                    time.sleep(args.delay)

    if not supports_move:
        mail.expunge()

    clear_checkpoint()
    logger.info(f"Completed. {success_count} messages moved.")

    mail.logout()


if __name__ == "__main__":
    move_to_trash()

