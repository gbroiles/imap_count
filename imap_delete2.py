#!/usr/bin/env python3
import imaplib
import os
import sys
import socket
from tqdm import tqdm

def move_to_trash_explicit_fast():
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
    success_count = 0

    try:
        try:
            mail = imaplib.IMAP4_SSL('imap.gmail.com')
        except socket.error as e:
            print(f"Network error: Could not connect to IMAP server. Details: {e}", file=sys.stderr)
            sys.exit(1)

        try:
            mail.login(user, password)
        except imaplib.IMAP4.error as e:
            print(f"Authentication failed. Details: {e}", file=sys.stderr)
            sys.exit(1)

        status, _ = mail.select(f'"{source_mailbox}"')
        if status != 'OK':
            print(f"Error: Could not select mailbox '{source_mailbox}'.", file=sys.stderr)
            return

        search_criterion = f'(FROM "{sender_email}")'
        status, data = mail.search(None, search_criterion)
        
        if status != 'OK':
            print(f"Error: Search operation failed for sender '{sender_email}'.", file=sys.stderr)
            return

        mail_ids = data[0].split()
        total_emails = len(mail_ids)
        
        if not mail_ids:
            print(f"No emails found from {sender_email} in {source_mailbox}.")
            return

        print(f"Found {total_emails} emails. Moving to [Gmail]/Trash in batches...")

        trash_folder = '"[Gmail]/Trash"'
        chunk_size = 100 
        
        with tqdm(total=total_emails, desc="Processing", unit="msg") as pbar:
            for i in range(0, total_emails, chunk_size):
                chunk = mail_ids[i:i + chunk_size]
                id_str = b','.join(chunk)
                
                try:
                    copy_status, _ = mail.copy(id_str, trash_folder)
                    
                    if copy_status == 'OK':
                        store_status, _ = mail.store(id_str, '+FLAGS', '\\Deleted')
                        if store_status == 'OK':
                            success_count += len(chunk)
                        else:
                            tqdm.write("Warning: Failed to flag a batch as \\Deleted.", file=sys.stderr)
                    else:
                        tqdm.write(f"Warning: Failed to copy a batch to Trash. Verify '{trash_folder}' exists.", file=sys.stderr)
                
                except imaplib.IMAP4.error as e:
                    tqdm.write(f"IMAP error processing batch: {e}", file=sys.stderr)
                
                pbar.update(len(chunk))

        if success_count > 0:
            print("Expunging deleted messages from source mailbox... ", end="", flush=True)
            try:
                mail.expunge()
                print(f"Done.\nSuccess: {success_count} emails moved to Trash and expunged from {source_mailbox}.")
            except imaplib.IMAP4.error as e:
                print(f"\nError during expunge operation: {e}", file=sys.stderr)

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user (Ctrl+C). Exiting gracefully...")
        if success_count > 0:
            print(f"Note: {success_count} emails were successfully copied and flagged for deletion.")
            print("Because the script was interrupted before expunging, these emails may still appear in the source folder until the next expunge operation.")

    except Exception as e:
        print(f"\nAn unexpected critical error occurred: {e}", file=sys.stderr)
    
    finally:
        if mail:
            try:
                if mail.state == 'SELECTED':
                    mail.close()
                mail.logout()
            except Exception:
                pass

if __name__ == "__main__":
    move_to_trash_explicit_fast()


