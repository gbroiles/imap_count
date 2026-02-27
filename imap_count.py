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
from email.utils import parseaddr
from collections import Counter
from tqdm import tqdm

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
    logging.warning("Interrupt received. Gracefully shutting down connections...")
    print("\nInterrupt received. Halting and cleaning up...")
    shutdown_flag.set()
    
    with connection_lock:
        for conn in active_connections:
            try:
                conn.logout()
            except Exception as e:
                logging.debug(f"Error during shutdown logout: {e}")
    sys.exit(0)

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
                self.mail.select(self.current_folder, readonly=self.readonly)
        except Exception as e:
            logging.error(f"Connection failed: {e}")

    def select(self, folder, readonly=False):
        self.current_folder = folder
        self.readonly = readonly
        return self.mail.select(folder, readonly=readonly)

    def _retry_operation(self, op_name, *args, **kwargs):
        last_exception = None
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
                    time.sleep(2)
                    self._connect()
        
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
        conn.select(folder, readonly=True)
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
                    try:
                        header_str = response_part[1].decode('utf-8', errors='ignore')
                        if header_str.lower().startswith('from:'):
                            name, email_address = parseaddr(header_str[5:])
                            if email_address:
                                senders.append(email_address.lower())
                    except Exception as e:
                        logging.debug(f"Failed to parse header: {e}")
                        continue
        else:
            logging.warning(f"Fetch command returned status: {status}")
            
    except Exception as e:
        logging.error(f"Exception during chunk fetch: {e}")
        
    return senders


def list_top_senders(username, password, imap_server, folder="INBOX"):
    try:
        logging.info("Initializing main connection to retrieve message IDs.")
        main_conn = ResilientIMAP(imap_server, username, password, timeout=CONNECTION_TIMEOUT)
        status, _ = main_conn.select(folder, readonly=True)
        if status != 'OK':
            logging.error(f"Failed to select folder: {folder}")
            print(f"Error: Could not select folder '{folder}'. Check logs.")
            return

        status, messages = main_conn.search(None, 'ALL')
        if status != 'OK':
            logging.error("Failed to retrieve messages via search command.")
            print("Error: Could not retrieve messages. Check logs.")
            return

        email_ids = messages[0].split()
        main_conn.logout()
    except Exception as e:
        logging.critical(f"Fatal initialization error: {e}")
        print("Fatal error during initialization. Check logs.")
        return

    logging.info(f"Starting to process {len(email_ids)} messages across {MAX_WORKERS} threads.")
    print(f"Processing {len(email_ids)} messages (Press Ctrl+C to abort)...")

    chunks = [email_ids[i:i + CHUNK_SIZE] for i in range(0, len(email_ids), CHUNK_SIZE)]
    all_senders = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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

    if not shutdown_flag.is_set():
        logging.info("Processing complete. Cleaning up connections.")
        with connection_lock:
            for conn in active_connections:
                conn.logout()

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
    
    IMAP_HOST = "imap.gmail.com"
    USER_EMAIL = os.getenv('GMAIL_ACCT')
    USER_PASS = os.getenv('GMAIL_PASS')
    
    if len(sys.argv) > 1:
        TARGET_FOLDER = sys.argv[1]
    else:
        print("Usage: python3 imap_count.py <TARGET_FOLDER>")
        sys.exit(1)

    logging.info(f"Script started. Target folder: {TARGET_FOLDER}")
    list_top_senders(USER_EMAIL, USER_PASS, IMAP_HOST, TARGET_FOLDER)
    logging.info("Script execution finished.")


