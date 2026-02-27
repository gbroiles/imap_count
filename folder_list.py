#!/usr/bin/env python3
import imaplib
import os
import sys

def get_gmail_folders():
    # Retrieve credentials from environment variables
    user = os.getenv('GMAIL_ACCT')
    password = os.getenv('GMAIL_PASS')

    if not user or not password:
        print("Error: GMAIL_ACCT or GMAIL_PASS environment variables not set.")
        sys.exit(1)

    try:
        # Connect to Gmail's IMAP server
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(user, password)
        
        # Retrieve the list of folders/labels
        # list() returns a tuple: (status, [list of folders])
        status, folders = mail.list()

        if status == 'OK':
            print(f"Folders for {user}:")
            for folder in folders:
                # The folder string contains flags and the delimiter; 
                # we decode and print the full line.
                print(folder.decode('utf-8'))
        else:
            print("Failed to retrieve folders.")

        mail.logout()

    except imaplib.IMAP4.error as e:
        print(f"IMAP Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    get_gmail_folders()
