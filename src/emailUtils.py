# Databricks notebook source
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import email.utils

#emailUtilsKeyVaultScope
#Api_key = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="testSecret")
SENDER = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="SENDER")
SENDERNAME = 'AI LAB CFIA'
USERNAME_SMTP = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="USERNAMESMTP")
PASSWORD_SMTP = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="PASSWORDSMTP")
HOST = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="HOST")
PORT = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="PORT")

Title="multipart test"

#def send_email():
def send_email(receiver_email,dfInsecta, dfPlantae, dfMollusca, dfFungi, dfChromista, dfOthers, dfInsecta_us, dfMollusca_us, dfPlantae_us, dfFungi_us, dfChromista_us, dfOthers_us, Title="multipart test"):
    message = MIMEMultipart("alternative")
    message["Subject"] = Title
    message['From'] = email.utils.formataddr((SENDERNAME, SENDER))
    message["To"] = receiver_email
    
    
    dataframes=[dfInsecta, dfPlantae, dfMollusca, dfFungi, dfChromista, dfOthers]
    categories=["Insecta","Plantae","Mollusca","Fungi","Chromista","Others (recently added species which do not belong to the above) .."]
    dataframes_us=[dfInsecta_us, dfMollusca_us, dfPlantae_us, dfFungi_us, dfChromista_us, dfOthers_us]
    categories_us=["Insecta","Plantae","Mollusca","Fungi","Chromista","Others (recently added species which do not belong to the above) .."]
    table_index=0
    
    
    
    text = """\
        <html>
          <head></head>
          <body>""" 
          
    text_end = "" # TODO
        
    
    count=0
    for i in range(6) :
        df=dataframes[i]
        if len(df)!=0: 
            name=categories[i]
            
            tablelocation="""
            <p style="font-family:verdana"><br>
               <b>iconic_taxa: """+ name + """ </b>    
               <br>
               {0}
               <p>&nbsp;</p> 
           """
           
 
            tablelocation=tablelocation.format(df.to_html(render_links=True, justify='center'))
            text=text+tablelocation
            print(count)
            count=count+1
        
    for i in range(6) :
        df=dataframes_us[i]
        if len(df)!=0: 
            name=categories_us[i]
            
            tablelocation="""
            <p style="font-family:verdana"><br>
               <b>US iconic_taxa: """+ name + """ </b>    
               <br>
               {0}
               <p>&nbsp;</p> 
           """
           
 
            tablelocation=tablelocation.format(df.to_html(render_links=True, justify='center'))
            text=text+tablelocation
            print(count)
            count=count+1
    if count == 0:
      zero_text = """
      <p>
        There was no invasive specie found 
      </p>"""
      text = text + zero_text
    text=text+text_end 
        

    text=re.sub('target="_blank".+>', '> URL </a> ', text)
    part1 = MIMEText(text, "html")
    
    

    message.attach(part1)

    try:

        server = smtplib.SMTP(HOST, PORT)

        server.ehlo()

        server.starttls()

        #stmplib docs recommend calling ehlo() before & after starttls()

        server.ehlo()

        server.login(USERNAME_SMTP, PASSWORD_SMTP)

        server.sendmail(SENDER, receiver_email, message.as_string())

        server.close()

    # Display an error message if something goes wrong.

    except Exception as e:
        print ("Error: ", e)

    else:
        print ("Email sent!")
        