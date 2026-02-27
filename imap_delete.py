#!/usr/bin/env python3
import imaplib
import pandas as pd
import json
import yaml
import logging
from tqdm import tqdm
import sys
import os

def load_credentials():
    try:
            user = os.getenv('GMAIL_ACCT')
            password = os.getenv('GMAIL_PASS')
            return user, password
    except Exception as e:
        logging.error("Failed to load credentials: {}".format(e))
        raise


def connect_to_gmail_imap(user, password):
    imap_url = 'imap.gmail.com'
    try:
        mail = imaplib.IMAP4_SSL(imap_url)
        mail.login(user, password)
        mail.select('inbox')  # Connect to the inbox.
        return mail
    except Exception as e:
        logging.error("Connection failed: {}".format(e))
        raise

def get_emails_to_delete(mail, filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)
        emails_to_delete = data['emails']

    summary = pd.DataFrame(columns=['Email', 'Count'])
    rows = []
    for email in tqdm(emails_to_delete):
        _, messages = mail.search(None, 'FROM "{}"'.format(email))
#        print(type(messages[0]))
#        sys.exit(1)
#        message_set = ','.join(messages)
        print(repr(messages))
        print(type(messages))
        print(len(messages))
        if messages[0]  != b'':
            message_set = b','.join(messages).decode()  # "1,2,3"
            print("Deleting:", repr(message_set))
            typ, resp = mail.store(message_set, '+FLAGS', r'(\Deleted)')
            print("STORE:", typ, resp)

#            if typ == 'OK':
#                mail.expunge()

       # mail.store(messages, '+FLAGS', '\\Deleted')
        rows.append({'Email': email, 'Count': len(messages)})
    summary = pd.DataFrame(rows)
    return summary

def main():
    credentials = load_credentials()
    mail = connect_to_gmail_imap(*credentials)
    summary = get_emails_to_delete(mail, 'delete_list.json')
    print(summary)
    
if __name__ == "__main__":
    main()
