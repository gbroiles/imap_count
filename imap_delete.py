#!/usr/bin/env python3
import imaplib
import pandas as pd
import json
import yaml
import logging
import tqdm

def load_credentials(filepath):
    try:
        with open(filepath, 'r') as file:
            credentials = yaml.safe_load(file)
            user = credentials['user']
            password = credentials['password']
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
    for email in tqdm(emails_to_delete):
        _, messages = mail.search(None, 'FROM "{}"'.format(email))
        mail.store(messages, '+FLAGS', '\\Deleted')
        summary = summary.append({'Email': email, 'Count': len(messages)}, ignore_index=True)
    return summary

def main():
    credentials = load_credentials(argv[1])
    mail = connect_to_gmail_imap(*credentials)
    summary = get_emails_to_delete(mail, 'path_to_email_list.json')
    print(summary)
    
if __name__ == "__main__":
    main()
