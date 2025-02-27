# Databricks notebook source
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import email.utils
from dotenv import load_dotenv
import os

load_dotenv()

SENDER = os.getenv("SENDER")
SENDERNAME = 'AI LAB CFIA'
USERNAME_SMTP = os.getenv("USERNAME_SMTP")
PASSWORD_SMTP = os.getenv("PASSWORD_SMTP")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

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
          
    text_end = """    
               <br>
               <br>
               <p>
               <a href="https://github.com/ai-cfia/">Ai Lab</a> <br>
               Innovation, Business, and Service Development Branch<br>
               Canadian Food Inspection Agency / Government of Canada<br>
               <a href = "mailto:cfia.ai-ia.acia@canada.ca?subject = EarlyWarning = Message">
               cfia.ai-ia.acia@canada.ca
               </a><br>
        
               laboratoire d’intelligence artificielle<br>
               Direction générale du développement des affaires, des services et de l'innovation<br>
               Agence canadienne d'inspection des aliments / Gouvernement du Canada<br>
               <a href = "mailto:cfia.ai-ia.acia@canada.ca?subject = EarlyWarning = Message">
               cfia.ai-ia.acia@canada.ca
               </a><br>
               <br>
               
               
               <p><small>
                   
               You are receiving this email because you have expressed interest in including your email address in the "early warning" project mailing list.<br>
               It is important to note that we are sending these emails from a Gmail address only as part of the pilot project. When the final version of this project will be deployed, you will receive these emails from the official Ai Lab address.
               Want to change how you receive these emails? please send us an email at <a href = "mailto:cfia.ai-ia.acia@canada.ca?subject = EarlyWarning = Message">
               cfia.ai-ia.acia@canada.ca</a></p>
               <p>
               Vous recevez ce courriel parce que vous avez exprimé votre intérêt pour inclure votre adresse courriel dans la liste d'envoi du projet "early warning".<br>
               Il est important de noter que nous envoyons ces courriels à partir d'une adresse Gmail seulement dans le cadre du projet pilote. Lorsque la version finale de ce projet sera déployée, vous recevrez ces courriels à partir de l'adresse officielle du Ai Lab.
               Si vous ne souhaitez plus recevoir ce message, veuillez nous envoyer un courriel à <a href = "mailto:cfia.ai-ia.acia@canada.ca?subject = EarlyWarning = Message">
               cfia.ai-ia.acia@canada.ca</a> </p>
               
               </small></p>
            </p>
          </body>
        </html>
        """
        
    
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
        