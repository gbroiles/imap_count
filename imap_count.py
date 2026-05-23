#!/usr/bin/env python3
import imaplib
import os
import sys
import time
import socket
import threading
import concurrent.futures
import signal
import logging
import argparse
from email import message_from_bytes
from email.utils import parseaddr
from collections import Counter
from tqdm import tqdm

imaplib._MAXLINE = 100000000
CONNECTION_TIMEOUT = 60
MAX_WORKERS = 5
CHUNK_SIZE = 1000

# Configure logging
logging.basicConfig(
    filename='imap_processor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

shutdown_flag = threading.Event()
active_connections = []
connection_lock = threading.Lock()

def signal_handler(sig, frame):
    logging.warning("Interrupt received. Gracefully shutting down...")
    print("\nInterrupt received. Halting and cleaning up...")
    shutdown_flag.set()
    # Close open sockets so blocking IMAP I/O in worker threads fails fast
    # instead of waiting up to CONNECTION_TIMEOUT seconds per thread.
    with connection_lock:
        for conn in active_connections:
            try:
                conn.mail.shutdown()
            except Exception:
                pass


class ResilientIMAP:
    def __init__(self, host, user, password, timeout=60, retries=3):
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

        try:
            self.mail = imaplib.IMAP4_SSL(self.host, timeout=self.timeout)
            self.mail.login(self.user, self.password)
            logging.info("Successfully established IMAP connection.")

            if self.current_folder:
                status, _ = self.mail.select(self.current_folder, readonly=self.readonly)
                if status != 'OK':
                    raise ConnectionError(f"Failed to reselect folder '{self.current_folder}' after reconnect")
        except ConnectionError:
            raise
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            raise ConnectionError(f"Failed to connect to {self.host}") from e

    def select(self, folder, readonly=False):
        self.current_folder = folder
        self.readonly = readonly
        status, data = self.mail.select(folder, readonly=readonly)
        if status != 'OK':
            raise ConnectionError(f"Failed to select folder '{folder}'")
        return status, data

    def _retry_operation(self, op_name, *args, **kwargs):
        last_exception = RuntimeError(f"Operation '{op_name}' failed after all retries")
        for attempt in range(self.retries):
            if shutdown_flag.is_set():
                return 'ABORT', []

            try:
                func = getattr(self.mail, op_name)
                return func(*args, **kwargs)
            except (imaplib.IMAP4.abort, socket.error, EOFError) as e:
                last_exception = e
                logging.warning(f"Operation '{op_name}' failed ({e}). Retrying {attempt + 1}/{self.retries}...")
                if attempt < self.retries - 1:
                    if shutdown_flag.is_set():
                        return 'ABORT', []
                    time.sleep(2)
                    try:
                        self._connect()
                    except Exception as reconnect_err:
                        last_exception = reconnect_err
                        logging.warning(f"Reconnect attempt {attempt + 1} failed: {reconnect_err}")

        logging.error(f"Operation '{op_name}' exhausted retries. Last error: {last_exception}")
        raise last_exception

    def search(self, *args, **kwargs):
        return self._retry_operation('search', *args, **kwargs)

    def fetch(self, *args, **kwargs):
        return self._retry_operation('fetch', *args, **kwargs)

    def logout(self):
        try:
            self.mail.close()
        except Exception:
            pass
        try:
            self.mail.logout()
            logging.info("IMAP connection closed and logged out.")
        except Exception as e:
            logging.debug(f"Logout exception: {e}")


thread_local = threading.local()

def get_thread_connection(host, user, password, folder):
    if not hasattr(thread_local, "mail"):
        logging.info("Initializing new thread-local IMAP connection.")
        conn = ResilientIMAP(host, user, password, timeout=CONNECTION_TIMEOUT)
        try:
            conn.select(folder, readonly=True)
        except Exception:
            conn.logout()
            raise
        thread_local.mail = conn
        with connection_lock:
            active_connections.append(conn)
    return thread_local.mail

def fetch_chunk(chunk, host, user, password, folder):
    if shutdown_flag.is_set():
        return []

    mail = get_thread_connection(host, user, password, folder)
    fetch_ids = b','.join(chunk)
    senders = []

    try:
        status, msg_data = mail.fetch(fetch_ids, '(BODY[HEADER.FIELDS (FROM)])')

        if status == 'OK':
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    if response_part[1] is None:
                        continue
                    try:
                        msg = message_from_bytes(response_part[1])
                        _, email_address = parseaddr(msg.get('From', ''))
                        if email_address:
                            senders.append(email_address.lower())
                    except Exception as e:
                        logging.debug(f"Failed to parse header: {e}")
                        continue
        elif status == 'ABORT':
            logging.debug("Fetch aborted due to shutdown.")
        else:
            logging.warning(f"Fetch command returned status: {status}")

    except imaplib.IMAP4.error as e:
        logging.error(f"IMAP protocol error during chunk fetch (server rejected command): {e}")
        if hasattr(thread_local, 'mail'):
            del thread_local.mail
    except Exception as e:
        logging.error(f"Exception during chunk fetch: {e}")
        if hasattr(thread_local, 'mail'):
            del thread_local.mail

    return senders


def list_top_senders(username, password, imap_server, folder="INBOX"):
    main_conn = None
    try:
        logging.info("Initializing main connection to retrieve message IDs.")
        main_conn = ResilientIMAP(imap_server, username, password, timeout=CONNECTION_TIMEOUT)
        main_conn.select(folder, readonly=True)

        status, messages = main_conn.search(None, 'ALL')
        if status == 'ABORT':
            return
        if status != 'OK':
            logging.error("Failed to retrieve messages via search command.")
            print("Error: Could not retrieve messages. Check logs.")
            return

        email_ids = messages[0].split() if messages else []
    except Exception as e:
        logging.critical(f"Fatal initialization error: {e}")
        print(f"Fatal error during initialization: {e}. Check logs.")
        return
    finally:
        if main_conn is not None:
            main_conn.logout()

    logging.info(f"Starting to process {len(email_ids)} messages across {MAX_WORKERS} threads.")
    print(f"Processing {len(email_ids)} messages (Press Ctrl+C to abort)...")

    chunks = [email_ids[i:i + CHUNK_SIZE] for i in range(0, len(email_ids), CHUNK_SIZE)]
    all_senders = []

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
    try:
        futures = {executor.submit(fetch_chunk, chunk, imap_server, username, password, folder): chunk for chunk in chunks}

        with tqdm(total=len(email_ids)) as pbar:
            for future in concurrent.futures.as_completed(futures):
                if shutdown_flag.is_set():
                    break

                chunk = futures[future]
                try:
                    chunk_senders = future.result()
                    all_senders.extend(chunk_senders)
                except Exception as e:
                    logging.error(f"Chunk execution failed completely: {e}")

                pbar.update(len(chunk))
    finally:
        if shutdown_flag.is_set():
            if sys.version_info >= (3, 9):
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                executor.shutdown(wait=False)
        else:
            executor.shutdown(wait=True)

    logging.info("Processing complete. Cleaning up connections.")
    with connection_lock:
        for conn in active_connections:
            try:
                conn.logout()
            except Exception:
                pass
        active_connections.clear()

    if shutdown_flag.is_set():
        sys.exit(0)

    sender_counts = Counter(all_senders)
    filtered_sorted_senders = sorted(
        [(sender, count) for sender, count in sender_counts.items() if count > 1],
        key=lambda item: item[1],
        reverse=True
    )

    print("\n--- Sender Statistics ---")
    if not filtered_sorted_senders:
        print("No senders with more than 1 message found.")
    else:
        for sender, count in filtered_sorted_senders:
            print(f"{count:4d} | {sender}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description="Count top senders in a specific IMAP folder.")
    parser.add_argument("folder", help="Target IMAP folder to scan (e.g., INBOX)")
    parser.add_argument("-u", "--user", default=os.getenv('GMAIL_ACCT'), help="IMAP username (defaults to GMAIL_ACCT env var)")
    parser.add_argument("-p", "--password", default=os.getenv('GMAIL_PASS'), help="IMAP password (defaults to GMAIL_PASS env var)")
    parser.add_argument("-s", "--server", default="imap.gmail.com", help="IMAP server (defaults to imap.gmail.com)")

    args = parser.parse_args()

    if not args.user or not args.password:
        print("Error: Username and password must be provided via command-line arguments or environment variables.")
        sys.exit(1)

    logging.info(f"Script started. Server: {args.server}, Target folder: {args.folder}")
    list_top_senders(args.user, args.password, args.server, args.folder)
    logging.info("Script execution finished.")
