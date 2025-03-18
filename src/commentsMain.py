# Databricks notebook source
# MAGIC %run ./testscript

# COMMAND ----------

# MAGIC %run ./preprocessUtils

# COMMAND ----------

# MAGIC %run ./commentsUtils

# COMMAND ----------

# MAGIC %run ./commentsFlagUtils

# COMMAND ----------

# MAGIC %run ./commentsEmailUtils

# COMMAND ----------

import pandas as pd
'''
from commentsUtils import getQueryPairs
from apiUtils import getYesterday
from commentsEmailUtils import send_email
from preprocessUtils import FilterByLocaiton
from preprocessUtils import SampleImg
from commentsUtils import getDates
from commentsUtils import computeBoundingBoxes
from commentsUtils import load_obj
from commentsUtils import save_obj
from commentsUtils import getComments
from commentsUtils import getObsvsInfo
from commentsFlagUtils import preprocessComments
from commentsFlagUtils import flagList
'''

# COMMAND ----------

interval=7
iconic_taxa='insecta' 
maxCountTrials=100 
commentStatusPath = 'commentStatus' 
queryPairsPath = 'queryPairs_updated' 
toBeFlaggedList =[r"\b(new specie)\b",
                  r"\b(first record)\b",
                  r"\b(first detection)\b",
                  r"\b(first canadian)\b",  # species . record ? 
                  r"\b(first north american)\b",
                  r"\b(new)\b",
                  r"\b(brand new)\b",
                  r"\b(unknown)\b",
                  r"\b(cfia)\b",
                  r"\b(canadian food inspection agency)\b"
        ]
receiver_email = [] # TODO
################################# load commentStatus and queryPairs
try: 
    commentStatus = load_obj(commentStatusPath)
except: 
    commentStatus = pd.DataFrame(columns= ["observation_id","comment_count","country" ])
    
try:
    queryPairs = load_obj(queryPairsPath)
except: 
    queryPairs = {}
########################################################## 
########################################################## 
########################################################## 
# original bounding box
nelat=83.1000000
nelng=-050.7500000
swlat=41.2833333
swlng=-140.8833333
NE=[nelng, nelat ] 
SW= [swlng,  swlat] 
boundingbox= [SW, NE]
#############################
cols = ["Quality", "Name1", "Name2", "Obsvn_URL","Sample_Img","coordinates","posted_on","observed_on"] #"comments"]
cols_ordered = ['comments', 'Name1', 'Name2', 'City', 'Province', 'Obsvn_URL', 'Sample_Img', 'posted_on','flagged_terms']
################################# prep interval   
dates, newDates = getDates(interval, queryPairs) # queryPairs_dateNew
################################# compute bounding boxes for new dates 
boundingBoxOfDate = computeBoundingBoxes(iconic_taxa, boundingbox, newDates)
################################# compute queryPairs for new dates 
queryPairs = getQueryPairs (queryPairs, newDates, boundingBoxOfDate)
################################# save new query pairs 
save_obj(queryPairs, queryPairsPath)
########################################################## 
########################################################## 
########################################################## 
################################# get new comments
queryIsComplete, allComments, commentStatus = getComments(maxCountTrials, dates, queryPairs, commentStatus, iconic_taxa)

if queryIsComplete:
    
    
    ################################# clean comments 
    allComments=preprocessComments(allComments)
    ################################# flag list of terms 
    flaggedComments = flagList(toBeFlaggedList, allComments )
    ################################# get observation info of flagged comments
    flaggedComments = getObsvsInfo (flaggedComments,cols)
    ################################# clean table to be sent by email 
    flaggedComments=FilterByLocaiton(flaggedComments)
    flaggedComments=SampleImg(flaggedComments)
    flaggedComments=flaggedComments.drop(columns=['id','Quality','observed_on'])
    flaggedComments=flaggedComments[cols_ordered] # order
    ################################# update comment status 
    save_obj(commentStatus, commentStatusPath)
    ############## send email 
    
    MLcolumn="ai_visionVerification"
    sender_email='earlywarning.invasive@gmail.com'
    ############################## fixed
    datefrom=getYesterday()
    Title="Flagged Comments Submitted in the " +str(interval) +" Days Preceding to " + datefrom
    for remail in receiver_email:
        send_email(remail,flaggedComments,Title)
else: 
    Title="Comment Query Failed, StartDate: " + datefrom
    flaggedComments= pd.DataFrame()
    for remail in receiver_email_admin:
        send_email(remail,flaggedComments,Title)
