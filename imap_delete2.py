#!/usr/bin/env python3
import imaplib
import os
import sys
import socket
import logging
from tqdm import tqdm

imaplib._MAXLINE = 100000000
CONNECTION_TIMEOUT = 60

# Configure logging: INFO and above to console, WARNING and above to file
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

def move_to_trash_explicit_fast():
    if len(sys.argv) < 3:
        logger.error("Usage: python3 script.py <mailbox_name> <sender_email>")
        sys.exit(1)

    # Reordered arguments: Mailbox first, then sender email
    source_mailbox = sys.argv[1]
    sender_email = sys.argv[2]

    user = os.getenv('GMAIL_ACCT')
    password = os.getenv('GMAIL_PASS')

    if not user or not password:
        logger.error("Error: GMAIL_ACCT or GMAIL_PASS environment variables not set.")
        sys.exit(1)

    mail = None
    success_count = 0

    try:
        try:
            mail = imaplib.IMAP4_SSL('imap.gmail.com', timeout=CONNECTION_TIMEOUT)
        except socket.error as e:
            logger.error(f"Network error: Could not connect to IMAP server. Details: {e}")
            sys.exit(1)

        try:
            mail.login(user, password)
        except imaplib.IMAP4.error as e:
            logger.error(f"Authentication failed. Details: {e}")
            sys.exit(1)

        status, _ = mail.select(f'"{source_mailbox}"')
        if status != 'OK':
            logger.error(f"Error: Could not select mailbox '{source_mailbox}'.")
            return

        search_criterion = f'(FROM "{sender_email}")'
        status, data = mail.search(None, search_criterion)
        
        if status != 'OK':
            logger.error(f"Error: Search operation failed for sender '{sender_email}'.")
            return

        mail_ids = data[0].split()
        total_emails = len(mail_ids)
        
        if not mail_ids:
            logger.info(f"No emails found from {sender_email} in {source_mailbox}.")
            return

        logger.info(f"Found {total_emails} emails. Moving to [Gmail]/Trash in batches...")

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
                            logger.warning(f"Batch {i//chunk_size}: Failed to flag as \\Deleted.")
                    else:
                        logger.warning(f"Batch {i//chunk_size}: Failed to copy to Trash. Verify '{trash_folder}' exists.")
                
                except imaplib.IMAP4.abort as e:
                    logger.error(f"Fatal IMAP connection error on batch {i//chunk_size}: {e}")
                    break # Break loop if the connection completely drops
                except Exception as e:
                    # Catch individual batch errors and continue processing the rest
                    logger.error(f"Unexpected error processing batch {i//chunk_size}: {e}")
                    pass 
                
                pbar.update(len(chunk))

        if success_count > 0:
            logger.info("Expunging deleted messages from source mailbox...")
            try:
                mail.expunge()
                logger.info(f"Success: {success_count} emails moved to Trash and expunged from {source_mailbox}.")
            except Exception as e:
                logger.error(f"Error during expunge operation: {e}")

    except KeyboardInterrupt:
        print() # Add a newline to separate from tqdm output
        logger.warning("Operation cancelled by user (Ctrl+C). Shutting down gracefully...")
        if success_count > 0:
            logger.info(f"Note: {success_count} emails were copied and flagged for deletion.")
            logger.info("Because the script was interrupted before expunging, these emails may still appear in the source folder until the next expunge operation.")

    except Exception as e:
        logger.error(f"An unexpected critical error occurred: {e}")
    
    finally:
        if mail:
            try:
                if mail.state == 'SELECTED':
                    mail.close()
                mail.logout()
            except Exception as e:
                logger.error(f"Error during IMAP disconnect: {e}")

if __name__ == "__main__":
    move_to_trash_explicit_fast()
