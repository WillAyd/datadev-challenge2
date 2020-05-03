import smtplib 
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

smtpServer = "your smtp server"
smtpPort = "smtp server port"
smtpUsername = "username use to login to mail server"
smtpPassword = "your password"
fromAddress = "user@example.com"

def send_email(toAddress, html):   
    msg = MIMEMultipart()
    msg['From'] = fromAddress
    msg['To'] = toAddress
    msg['Subject'] = "Tableau Server Alert - Bad Field Names"

    body = MIMEText(html, 'html')
    msg.attach(body)

    text = msg.as_string()
    mailServer = smtplib.SMTP(smtpServer , smtpPort) 
    mailServer.starttls() 
    mailServer.login(smtpUsername , smtpPassword) 
    mailServer.sendmail(fromAddress, toAddress , text) 
    print(" \n Email Sent!") 
    mailServer.quit()

if __name__ == '__main__':
    # test1.py executed as script
    send_email("To email Address","html body")
