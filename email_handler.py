import base64
import os.path
from google.oauth2 import credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from pathlib import Path


import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import smtplib

#from email import encoders


def ReplyToMessage(service, message_id, sender):

    body = "message body"
    message_new = MIMEMultipart()
    message_new['to'] = sender
    message_new['subject'] = "my response"
    message_new.attach(MIMEText(body, 'plain'))
    raw_message = base64.urlsafe_b64encode(message_new.as_bytes()).decode('utf-8')
    send_message = service.users().messages().send(userId="me", body={'raw': raw_message}).execute()


    print(f"sent message to {message_new['to']} Message Id: {send_message['id']}")
    # reply_message = "Your reply message goes here."
    # message = {
    #     'raw': base64.urlsafe_b64encode(reply_message.encode('utf-8')).decode('utf-8'),
    #     'payload': {'headers': [{'name': 'to', 'value': sender}]}
    # }
    # send_message = service.users().messages().send(userId="me", body=message).execute()
    # print(F'sent message to {sender} Message Id: {send_message["id"]}')

def process_unread_messages():
    creds = None
    if os.path.exists('token.json'):
        creds = credentials.Credentials.from_authorized_user_file(Path('token.json'))

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', ['https://www.googleapis.com/auth/gmail.modify'])
            creds = flow.run_local_server(port=0)

        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)

    results = service.users().messages().list(userId='me', q='is:unread subject:"SVR"').execute()
    messages = results.get('messages', [])

    if not messages:
        print('No unread messages with the subject "SVR" found.')
    else:
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            payload = msg['payload']
            headers = payload['headers']
            subject = ''
            sender = ''
            for header in headers:
                if header['name'] == 'Subject':
                    subject = header['value']
                if header['name'] == 'From':
                    sender = header['value']

            if len(subject) >=3 and subject[:3] == 'SVR':
                ReplyToMessage(service, message['id'], sender)
                service.users().messages().modify(userId='me', id=message['id'], body={'removeLabelIds': ['UNREAD']}).execute()

if __name__ == '__main__':
    process_unread_messages()
