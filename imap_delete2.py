#!/usr/bin/env python3
import imaplib
import os
import sys
import socket
import logging
import time
import argparse
from tqdm import tqdm

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
    parser = argparse.ArgumentParser(description="Move emails from a specific sender to Trash.")
    parser.add_argument("mailbox", help="Source mailbox name (e.g., INBOX)")
    parser.add_argument("sender", help="Sender email address")
    parser.add_argument("--retries", type=int, default=3, help="Maximum number of retries per batch")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay in seconds between retries")
    parser.add_argument("--timeout", type=float, default=30.0, help="IMAP connection timeout in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the process without moving or deleting emails")
    
    args = parser.parse_args()

    user = os.getenv('GMAIL_ACCT')
    password = os.getenv('GMAIL_PASS')

    if not user or not password:
        logger.error("Error: GMAIL_ACCT or GMAIL_PASS environment variables not set.")
        sys.exit(1)

    mail = None
    success_count = 0

    try:
        try:
            # The timeout parameter in IMAP4_SSL is available in Python 3.9+
            mail = imaplib.IMAP4_SSL('imap.gmail.com', timeout=args.timeout)
        except TypeError:
            # Fallback for Python versions older than 3.9
            logger.warning("Using socket.setdefaulttimeout fallback for older Python version.")
            socket.setdefaulttimeout(args.timeout)
            mail = imaplib.IMAP4_SSL('imap.gmail.com')
        except socket.error as e:
            logger.error(f"Network error: Could not connect to IMAP server. Details: {e}")
            sys.exit(1)

        try:
            mail.login(user, password)
        except imaplib.IMAP4.error as e:
            logger.error(f"Authentication failed. Details: {e}")
            sys.exit(1)

        status, _ = mail.select(f'"{args.mailbox}"')
        if status != 'OK':
            logger.error(f"Error: Could not select mailbox '{args.mailbox}'.")
            return

        search_criterion = f'(FROM "{args.sender}")'
        status, data = mail.search(None, search_criterion)
        
        if status != 'OK':
            logger.error(f"Error: Search operation failed for sender '{args.sender}'.")
            return

        mail_ids = data[0].split()
        total_emails = len(mail_ids)
        
        if not mail_ids:
            logger.info(f"No emails found from {args.sender} in {args.mailbox}.")
            return

        # Handle the dry run immediately after counting the emails
        if args.dry_run:
            logger.info(f"[DRY RUN] Found {total_emails} emails from '{args.sender}' in '{args.mailbox}'.")
            logger.info("[DRY RUN] No emails were moved or deleted. Exiting safely.")
            return

        logger.info(f"Found {total_emails} emails. Moving to [Gmail]/Trash in batches...")

        trash_folder = '"[Gmail]/Trash"'
        chunk_size = 100 
        
        with tqdm(total=total_emails, desc="Processing", unit="msg") as pbar:
            for i in range(0, total_emails, chunk_size):
                chunk = mail_ids[i:i + chunk_size]
                id_str = b','.join(chunk)
                batch_success = False
                batch_index = i // chunk_size
                
                for attempt in range(args.retries):
                    try:
                        copy_status, _ = mail.copy(id_str, trash_folder)
                        
                        if copy_status == 'OK':
                            store_status, _ = mail.store(id_str, '+FLAGS', '\\Deleted')
                            if store_status == 'OK':
                                success_count += len(chunk)
                                batch_success = True
                                break 
                            else:
                                logger.warning(f"Batch {batch_index} (Attempt {attempt+1}): Failed to flag as \\Deleted.")
                        else:
                            logger.warning(f"Batch {batch_index} (Attempt {attempt+1}): Failed to copy to Trash.")
                    
                    except imaplib.IMAP4.abort as e:
                        logger.error(f"Fatal IMAP connection error on batch {batch_index}: {e}")
                        raise 
                    except Exception as e:
                        logger.warning(f"Error processing batch {batch_index} (Attempt {attempt+1}): {e}")
                    
                    if attempt < args.retries - 1:
                        time.sleep(args.delay)

                if not batch_success:
                    logger.error(f"Batch {batch_index} completely failed after {args.retries} attempts. Skipping to next batch.")
                
                pbar.update(len(chunk))

        if success_count > 0:
            logger.info(f"Expunging deleted messages from {args.mailbox}...")
            try:
                mail.expunge()
                logger.info(f"Success: {success_count} emails moved to Trash and expunged from {args.mailbox}.")
            except Exception as e:
                logger.error(f"Error during expunge operation: {e}")

    except KeyboardInterrupt:
        print() 
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
