#! /usr/bin/python

import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(
        From, To, Subject,
        html, text,
        smtp_hostname=None, smtp_port=None,
        username=None, password=None,
        google=False,
):
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = Subject
    msg['From'] = From
    msg['To'] = To

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    if not smtp_hostname:
        if From.endswith('@gmail.com') or google:
            smtp_hostname = 'smtp.gmail.com'
        else:
            raise Exception("Couldn't find or infer SMTP server URL")

    smtp_port = smtp_port or 587
    mail = smtplib.SMTP(smtp_hostname, smtp_port)

    mail.ehlo()

    mail.starttls()

    if username:
        if not password:
            raise Exception('No password passed')
    else:
        if password:
            username = From

    if username:
        mail.login(username, password)

    print('Emailing')
    mail.sendmail(From, To, msg.as_string())
    mail.quit()
