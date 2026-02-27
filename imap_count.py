#!/usr/bin/env python3
import imaplib
import email
import os
import sys
from email.utils import parseaddr
from collections import Counter
from tqdm import tqdm

CONNECTION_TIMEOUT = 60

def list_top_senders(username, password, imap_server, folder="INBOX"):
    """
    Connects to an IMAP account, fetches 'From' headers, and returns 
    a sorted list of senders with > 1 message.
    """
    try:
        # Connect to the IMAP server securely
        mail = imaplib.IMAP4_SSL(imap_server, timeout=CONNECTION_TIMEOUT)
        mail.login(username, password)
    except imaplib.IMAP4.error as e:
        print(f"Login failed: {e}")
        return

    # Select the target folder (Read-only mode for safety)
    status, _ = mail.select(folder, readonly=True)
    if status != 'OK':
        print(f"Failed to select folder: {folder}")
        return

    # Search for all messages in the selected folder
    status, messages = mail.search(None, 'ALL')
    if status != 'OK':
        print("Failed to retrieve messages.")
        return

    email_ids = messages[0].split()
    senders = []

    print(f"Processing {len(email_ids)} messages. This may take a moment...")

    # Fetch headers efficiently
    for e_id in tqdm(email_ids):
        # Fetch ONLY the 'From' header to minimize bandwidth and processing time
        status, msg_data = mail.fetch(e_id, '(BODY[HEADER.FIELDS (FROM)])')
        if status != 'OK':
            continue

        for response_part in msg_data:
            if isinstance(response_part, tuple):
                # Parse the byte data into an email message object
                msg = email.message_from_bytes(response_part[1])
                from_header = msg.get('From')
                
                if from_header:
                    # Extract the pure email address from formats like "Name <user@domain.com>"
                    name, email_address = parseaddr(from_header)
                    if email_address:
                        senders.append(email_address.lower())

    # Close the folder and logout
    mail.close()
    mail.logout()

    # Count occurrences of each sender
    sender_counts = Counter(senders)

    # Filter out senders with only 1 message and sort descending
    filtered_sorted_senders = sorted(
        [(sender, count) for sender, count in sender_counts.items() if count > 1],
        key=lambda item: item[1], 
        reverse=True
    )

    # Display the results
    print("\n--- Sender Statistics ---")
    if not filtered_sorted_senders:
        print("No senders with more than 1 message found.")
    else:
        for sender, count in filtered_sorted_senders:
            print(f"{count:4d} | {sender}")

if __name__ == "__main__":
    IMAP_HOST = "imap.gmail.com"  # e.g., imap.gmail.com, outlook.office365.com
    USER_EMAIL = os.getenv('GMAIL_ACCT')
    USER_PASS = os.getenv('GMAIL_PASS') # Use an App Password, not your standard password
#    TARGET_FOLDER = "INBOX"
    TARGET_FOLDER = sys.argv[1]

    list_top_senders(USER_EMAIL, USER_PASS, IMAP_HOST, TARGET_FOLDER)
