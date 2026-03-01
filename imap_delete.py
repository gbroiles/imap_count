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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("imap_errors.log")
file_handler.setLevel(logging.WARNING)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

try:
    os.chmod("imap_errors.log", stat.S_IRUSR | stat.S_IWUSR)
except OSError:
    pass


EMAIL_REGEX = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'


def validate_sender(sender: str) -> bool:
    return bool(re.fullmatch(EMAIL_REGEX, sender))


def find_trash_folder(mail):
    status, mailboxes = mail.list()
    if status != 'OK':
        raise RuntimeError("Unable to list mailboxes.")

    for mbox in mailboxes:
        decoded = mbox.decode()
        if '\\Trash' in decoded:
            # Mailbox name is last quoted string
            return decoded.split(' "/" ')[-1].strip('"')
    raise RuntimeError("No mailbox with \\Trash flag found.")


def move_to_trash():
    parser = argparse.ArgumentParser(description="Move emails from a specific sender to Trash.")
    parser.add_argument("mailbox")
    parser.add_argument("sender")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if not validate_sender(args.sender):
        logger.error("Invalid sender email format.")
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

        status, data = mail.uid("search", None, "FROM", args.sender)
        if status != "OK":
            logger.error("Search failed.")
            return

        uids = data[0].split()
        total = len(uids)

        if total == 0:
            logger.info("No matching messages found.")
            return

        if args.dry_run:
            logger.info(f"[DRY RUN] {total} messages would be moved.")
            return

        trash_folder = find_trash_folder(mail)
        logger.info(f"Moving {total} messages to '{trash_folder}'")

        supports_move = b"MOVE" in mail.capabilities
        chunk_size = 100

        with tqdm(total=total, unit="msg") as pbar:
            for i in range(0, total, chunk_size):
                chunk = uids[i:i + chunk_size]
                uid_str = b",".join(chunk)

                for attempt in range(args.retries):
                    try:
                        if supports_move:
                            status, _ = mail.uid("MOVE", uid_str, trash_folder)
                            if status == "OK":
                                success_count += len(chunk)
                                break
                        else:
                            # Fallback: COPY + STORE
                            c_status, _ = mail.uid("COPY", uid_str, trash_folder)
                            if c_status != "OK":
                                raise RuntimeError("COPY failed")

                            s_status, _ = mail.uid("STORE", uid_str, "+FLAGS", r"\Deleted")
                            if s_status != "OK":
                                raise RuntimeError("STORE failed")

                            success_count += len(chunk)
                            break

                    except Exception as e:
                        if attempt == args.retries - 1:
                            logger.error(f"Batch {i//chunk_size} failed: {e}")
                        else:
                            time.sleep(args.delay)

                pbar.update(len(chunk))

        if not supports_move and success_count > 0:
            logger.info("Expunging deleted messages...")
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

