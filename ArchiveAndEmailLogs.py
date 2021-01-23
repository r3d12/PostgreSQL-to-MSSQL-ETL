import zipfile
import os
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import *


def logHandler(email = []):
    #rename file with timestamp

    Filepath = 'C:\Projects\Python\CDK_DW DAB\DATA_MIGRATION_DAB.log'
    modifiedTime = os.path.getmtime(Filepath)
    timestamp = datetime.fromtimestamp(modifiedTime).strftime("%b-%d-%Y")

    prevName = 'C:\Projects\Python\CDK_DW DAB\DATA_MIGRATION_DAB.log'
    newName = 'C:\Projects\Python\CDK_DW DAB\DATA_MIGRATION_DAB'

    os.rename(prevName, newName + "_" + timestamp + ".log")
    filename = 'DATA_MIGRATION_DAB'+ "_" + timestamp + ".log"


    #Email
    body = "TXDC-SQL-01 DAB Nightly DATA Pull. Please Review Log for Errors"
    # Log in to server using secure context and send email
    msg = MIMEMultipart('alternative')
    msg['Subject'] = ' DATA PULL REPORT'
    msg['From'] = 'Systems <email@domain.com>'

    to=email

    for item in to:
        msg.add_header('To', item)

    part1 = (MIMEText(body, "plain"))


    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        
    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)
    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    msg.attach(part1)
    # Add attachment to message
    msg.attach(part)
    s = smtplib.SMTP('mail_server')
    # s.send_message(msg)
    s.send_message(msg, to_addrs = msg.get_all('To'))
    s.close()


    #compress logs
    os.chdir("C:\Projects\Python\CDK_DW DAB")
    zip1 = zipfile.ZipFile('DAB_LOGS.zip', 'a')
    zip1.write(filename, compress_type=zipfile.ZIP_DEFLATED)
    zip1.close()

    os.remove(filename)

