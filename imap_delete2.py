#!/usr/bin/env python3
import imaplib
import os
import sys
import socket

def move_to_trash_explicit():
    # 1. Validate inputs and environment
    if len(sys.argv) < 3:
        print("Usage: python3 script.py <sender_email> <mailbox_name>", file=sys.stderr)
        sys.exit(1)

    sender_email = sys.argv[1]
    source_mailbox = sys.argv[2]

    user = os.getenv('GMAIL_ACCT')
    password = os.getenv('GMAIL_PASS')

    if not user or not password:
        print("Error: GMAIL_ACCT or GMAIL_PASS environment variables not set.", file=sys.stderr)
        sys.exit(1)

    mail = None
    try:
        # 2. Handle Network Connection
        try:
            mail = imaplib.IMAP4_SSL('imap.gmail.com')
        except socket.error as e:
            print(f"Network error: Could not connect to IMAP server. Details: {e}", file=sys.stderr)
            sys.exit(1)

        # 3. Handle Authentication
        try:
            mail.login(user, password)
        except imaplib.IMAP4.error as e:
            print(f"Authentication failed. Verify credentials or App Password. Details: {e}", file=sys.stderr)
            sys.exit(1)

        # 4. Handle Mailbox Selection
        status, _ = mail.select(f'"{source_mailbox}"')
        if status != 'OK':
            print(f"Error: Could not select mailbox '{source_mailbox}'. Verify the mailbox exists.", file=sys.stderr)
            return

        # 5. Handle Search
        search_criterion = f'(FROM "{sender_email}")'
        status, data = mail.search(None, search_criterion)
        
        if status != 'OK':
            print(f"Error: Search operation failed for sender '{sender_email}'.", file=sys.stderr)
            return

        mail_ids = data[0].split()
        if not mail_ids:
            print(f"No emails found from {sender_email} in {source_mailbox}.")
            return

        print(f"Found {len(mail_ids)} emails. Moving to [Gmail]/Trash...")

        trash_folder = '"[Gmail]/Trash"'
        success_count = 0
        
        # 6. Handle Individual Message Operations
        for m_id in mail_ids:
            try:
                copy_status, _ = mail.copy(m_id, trash_folder)
                
                if copy_status == 'OK':
                    store_status, _ = mail.store(m_id, '+FLAGS', '\\Deleted')
                    if store_status == 'OK':
                        success_count += 1
                    else:
                        print(f"Warning: Failed to flag message ID {m_id.decode()} as \\Deleted.", file=sys.stderr)
                else:
                    print(f"Warning: Failed to copy message ID {m_id.decode()} to Trash. Verify '{trash_folder}' exists.", file=sys.stderr)
            
            except imaplib.IMAP4.error as e:
                print(f"IMAP error processing message ID {m_id.decode()}: {e}", file=sys.stderr)

        # 7. Handle Expunge
        if success_count > 0:
            try:
                mail.expunge()
                print(f"Success: {success_count} emails moved to Trash and expunged from {source_mailbox}.")
            except imaplib.IMAP4.error as e:
                print(f"Error during expunge operation: {e}", file=sys.stderr)

    except Exception as e:
        print(f"An unexpected critical error occurred: {e}", file=sys.stderr)
    
    # 8. Ensure Clean Disconnect
    finally:
        if mail:
            try:
                if mail.state == 'SELECTED':
                    mail.close()
                mail.logout()
            except Exception:
                pass

if __name__ == "__main__":
    move_to_trash_explicit()
