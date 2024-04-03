# Databricks notebook source
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import email.utils

# COMMAND ----------

#Api_key = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="testSecret")
SENDER = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="SENDER")
SENDERNAME = 'AI LAB CFIA'
USERNAME_SMTP = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="USERNAMESMTP")
PASSWORD_SMTP = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="PASSWORDSMTP")
HOST = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="HOST")
PORT = dbutils.secrets.get(scope="databricksKeyVaultv1Scope", key="PORT")

Title="multipart test"

def send_email(receiver_email, flaggedComments , Title="multipart test"):
    message = MIMEMultipart("alternative")
    message["Subject"] = Title
    message['From'] = email.utils.formataddr((SENDERNAME, SENDER))
    message["To"] = receiver_email
    
    dataframes=[flaggedComments]
    categories=["flagged comments"]
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
    for i in range(1) :
        df=dataframes[i]
        if len(df)!=0: 
            name=categories[i]
            
            tablelocation="""
            <p style="font-family:verdana"><br>
               <b>"""+ name + """ </b>    
               <br>
               {0}
               <p>&nbsp;</p> 
           """
           
 
            tablelocation=tablelocation.format(df.to_html(render_links=True, justify='center'))
            text=text+tablelocation
           
            count=count+1

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

'''  
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
'''