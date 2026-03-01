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
from tqdm import tqdm

imaplib.Debug = 0

LOG_FILE = "imap_errors.log"

# --- Secure log file creation (no race window) ---
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

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(console_handler)


EMAIL_REGEX = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'


def validate_sender(sender: str) -> bool:
    return bool(re.fullmatch(EMAIL_REGEX, sender))


def load_senders_from_file(path: str):
    if not os.path.isfile(path):
        raise ValueError("Sender file does not exist.")

    senders = set()

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            sender = line.strip()
            if not sender:
                continue
            if not validate_sender(sender):
                raise ValueError(
                    f"Invalid email format on line {line_no}: {sender}"
                )
            senders.add(sender)

    if not senders:
        raise ValueError("Sender file contains no valid email addresses.")

    return sorted(senders)


def find_trash_folder(mail):
    status, mailboxes = mail.list()
    if status != "OK":
        raise RuntimeError("Unable to list mailboxes.")

    for mbox in mailboxes:
        decoded = mbox.decode(errors="replace")
        if "\\Trash" in decoded:
            # Extract mailbox name safely
            parts = decoded.split(' "/" ')
            if len(parts) == 2:
                return parts[1].strip('"')

    raise RuntimeError("No mailbox with \\Trash flag found.")


# --- Hardened IMAP search query construction ---
def build_combined_from_query(senders):
    # Escape quotes defensively
    escaped = [s.replace('"', r'\"') for s in senders]

    if len(escaped) == 1:
        return f'(FROM "{escaped[0]}")'

    # Build nested OR tree: OR (FROM A) (OR (FROM B) (FROM C))
    query = f'(FROM "{escaped[-1]}")'
    for sender in reversed(escaped[:-1]):
        query = f'(OR (FROM "{sender}") {query})'
    return query


def move_to_trash():
    parser = argparse.ArgumentParser(
        description="Move emails from sender(s) to Trash."
    )
    parser.add_argument("mailbox")
    parser.add_argument("sender", nargs="?")
    parser.add_argument("--file")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if not args.sender and not args.file:
        logger.error("Provide either a sender email or --file.")
        sys.exit(1)

    if args.sender and args.file:
        logger.error("Provide either a sender email OR --file.")
        sys.exit(1)

    try:
        if args.file:
            senders = load_senders_from_file(args.file)
        else:
            if not validate_sender(args.sender):
                logger.error("Invalid sender email format.")
                sys.exit(1)
            senders = [args.sender]
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    user = os.getenv("GMAIL_ACCT")
    password = os.getenv("GMAIL_PASS")

    if not user or not password:
        logger.error("GMAIL_ACCT or GMAIL_PASS not set.")
        sys.exit(1)

    mail = None
    success_count = 0

    try:
        ssl_context = ssl.create_default_context()
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

        mail = imaplib.IMAP4_SSL(
            "imap.gmail.com",
            ssl_context=ssl_context,
            timeout=args.timeout,
        )

        mail.login(user, password)

        status, _ = mail.select(args.mailbox)
        if status != "OK":
            logger.error(f"Cannot select mailbox '{args.mailbox}'.")
            return

        search_query = build_combined_from_query(senders)

        status, data = mail.uid("search", None, search_query)
        if status != "OK":
            logger.error("Search failed.")
            return

        if not data or not data[0]:
            logger.info("No matching messages found.")
            return

        all_uids = data[0].split()
        total = len(all_uids)

        if args.dry_run:
            logger.info(f"[DRY RUN] {total} messages would be moved.")
            return

        trash_folder = find_trash_folder(mail)
        mail.capability()
        supports_move = b"MOVE" in mail.capabilities

        chunk_size = 100

        logger.info(f"Moving {total} messages to '{trash_folder}'")

        with tqdm(total=total, unit="msg") as pbar:
            for i in range(0, total, chunk_size):
                chunk = all_uids[i:i + chunk_size]
                uid_str = b",".join(chunk)

                for attempt in range(args.retries):
                    try:
                        if supports_move:
                            status, _ = mail.uid(
                                "MOVE", uid_str, trash_folder
                            )
                            if status != "OK":
                                raise RuntimeError("MOVE failed")
                        else:
                            c_status, _ = mail.uid(
                                "COPY", uid_str, trash_folder
                            )
                            if c_status != "OK":
                                raise RuntimeError("COPY failed")

                            s_status, _ = mail.uid(
                                "STORE",
                                uid_str,
                                "+FLAGS",
                                r"\Deleted",
                            )
                            if s_status != "OK":
                                raise RuntimeError("STORE failed")

                        success_count += len(chunk)
                        break

                    except Exception as e:
                        if attempt == args.retries - 1:
                            logger.error(
                                f"Batch {i//chunk_size} failed: {e}"
                            )
                        else:
                            time.sleep(args.delay)

                pbar.update(len(chunk))

        if not supports_move and success_count > 0:
            mail.expunge()

        logger.info(f"Completed. {success_count} messages moved.")

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    except Exception as e:
        logger.error(f"Critical error: {e}")
    finally:
        if mail:
            try:
                if mail.state == "SELECTED":
                    mail.close()
                mail.logout()
            except Exception:
                pass


if __name__ == "__main__":
    move_to_trash()
