#!/usr/bin/env python3
import imaplib
import email
import os
import sys
import time
import socket
from email.utils import parseaddr
from collections import Counter
from tqdm import tqdm

CONNECTION_TIMEOUT = 60
MINIMUM_COUNT = 10

class ResilientIMAP:
    """Wraps IMAP4_SSL to provide automatic reconnection and retries for specific operations."""
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
        """Establishes or re-establishes the IMAP connection."""
        if self.mail:
            try:
                self.mail.logout()
            except Exception:
                pass
        
        self.mail = imaplib.IMAP4_SSL(self.host, timeout=self.timeout)
        self.mail.login(self.user, self.password)
        
        if self.current_folder:
            self.mail.select(self.current_folder, readonly=self.readonly)

    def select(self, folder, readonly=False):
        self.current_folder = folder
        self.readonly = readonly
        return self.mail.select(folder, readonly=readonly)

    def _retry_operation(self, op_name, *args, **kwargs):
        """Executes an IMAP operation with retry logic."""
        last_exception = None
        for attempt in range(self.retries):
            try:
                func = getattr(self.mail, op_name)
                return func(*args, **kwargs)
            except (imaplib.IMAP4.abort, socket.error, EOFError) as e:
                last_exception = e
                if attempt < self.retries - 1:
                    time.sleep(2)
                    self._connect()
        raise last_exception

    def search(self, *args, **kwargs):
        return self._retry_operation('search', *args, **kwargs)

    def fetch(self, *args, **kwargs):
        return self._retry_operation('fetch', *args, **kwargs)

    def store(self, *args, **kwargs):
        return self._retry_operation('store', *args, **kwargs)

    def close(self):
        return self.mail.close()

    def logout(self):
        return self.mail.logout()


def list_top_senders(username, password, imap_server, folder="INBOX"):
    try:
        mail = ResilientIMAP(imap_server, username, password, timeout=CONNECTION_TIMEOUT)
    except imaplib.IMAP4.error as e:
        print(f"Login failed: {e}")
        return

    status, _ = mail.select(folder, readonly=True)
    if status != 'OK':
        print(f"Failed to select folder: {folder}")
        return

    status, messages = mail.search(None, 'ALL')
    if status != 'OK':
        print("Failed to retrieve messages.")
        return

    email_ids = messages[0].split()
    senders = []

    print(f"Processing {len(email_ids)} messages in batches. This may take a moment...")

    chunk_size = 100
    
    # Process in batches using a tqdm progress bar
    with tqdm(total=len(email_ids)) as pbar:
        for i in range(0, len(email_ids), chunk_size):
            chunk = email_ids[i:i + chunk_size]
            # IMAP fetch accepts comma-separated IDs
            fetch_ids = b','.join(chunk)
            
            # Fetch headers for the entire batch in one command
            status, msg_data = mail.fetch(fetch_ids, '(BODY[HEADER.FIELDS (FROM)])')
            
            if status == 'OK':
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])
                        from_header = msg.get('From')
                        
                        if from_header:
                            name, email_address = parseaddr(from_header)
                            if email_address:
                                senders.append(email_address.lower())
            
            pbar.update(len(chunk))

    mail.close()
    mail.logout()

    sender_counts = Counter(senders)

    filtered_sorted_senders = sorted(
        [(sender, count) for sender, count in sender_counts.items() if count >= MINIMUM_COUNT],
        key=lambda item: item[1], 
        reverse=True
    )

    print("\n--- Sender Statistics ---")
    if not filtered_sorted_senders:
        print(f"No senders with more than {MINIMUM_COUNT} message(s) found.")
    else:
        for sender, count in filtered_sorted_senders:
            print(f"{count:4d} | {sender}")

if __name__ == "__main__":
    IMAP_HOST = "imap.gmail.com"
    USER_EMAIL = os.getenv('GMAIL_ACCT')
    USER_PASS = os.getenv('GMAIL_PASS')
    
    if len(sys.argv) > 1:
        TARGET_FOLDER = sys.argv[1]
    else:
        print("Usage: python3 imap_count.py <TARGET_FOLDER>")
        sys.exit(1)

    list_top_senders(USER_EMAIL, USER_PASS, IMAP_HOST, TARGET_FOLDER)
