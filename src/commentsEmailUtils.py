# Databricks notebook source
import email.utils
import re
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# COMMAND ----------

# Api_key = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="testSecret")
SENDER = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="SENDER")
SENDERNAME = "AI LAB CFIA"
USERNAME_SMTP = dbutils.secrets.get(
    scope="databricksKeyVaultv1Scope", key="USERNAMESMTP"
)
PASSWORD_SMTP = dbutils.secrets.get(
    scope="databricksKeyVaultv1Scope", key="PASSWORDSMTP"
)
HOST = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="HOST")
PORT = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="PORT")

Title = "multipart test"


def send_email(receiver_email, flaggedComments, Title="multipart test"):
    message = MIMEMultipart("alternative")
    message["Subject"] = Title
    message["From"] = email.utils.formataddr((SENDERNAME, SENDER))
    message["To"] = receiver_email

    dataframes = [flaggedComments]
    categories = ["flagged comments"]
    table_index = 0

    text = """\
        <html>
          <head></head>
          <body>"""

    text_end = ""  # TODO

    count = 0
    for i in range(1):
        df = dataframes[i]
        if len(df) != 0:
            name = categories[i]

            tablelocation = (
                """
            <p style="font-family:verdana"><br>
               <b>"""
                + name
                + """ </b>    
               <br>
               {0}
               <p>&nbsp;</p> 
           """
            )

            tablelocation = tablelocation.format(
                df.to_html(render_links=True, justify="center")
            )
            text = text + tablelocation

            count = count + 1

    text = text + text_end

    text = re.sub('target="_blank".+>', "> URL </a> ", text)
    part1 = MIMEText(text, "html")

    message.attach(part1)

    try:
        server = smtplib.SMTP(HOST, PORT)

        server.ehlo()

        server.starttls()

        # stmplib docs recommend calling ehlo() before & after starttls()

        server.ehlo()

        server.login(USERNAME_SMTP, PASSWORD_SMTP)

        server.sendmail(SENDER, receiver_email, message.as_string())

        server.close()

    # Display an error message if something goes wrong.

    except Exception as e:
        print("Error: ", e)

    else:
        print("Email sent!")


"""  
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, message.as_string()
        )
        



a=commentsAllWindows['comments'].values
leng=[]
for c in a: 
    leng.append(len(c))
dates=[]
for comments in commentsAllWindows['comments'].values:
    date_i=[]
    for comment in comments: 
        a=comment['created_at_details']['date']
        date_i.append(a)
        
    dates.append(date_i)
commentsAllWindows['comments'][2:3].values[0][0]['created_at_details']['date']
"""
