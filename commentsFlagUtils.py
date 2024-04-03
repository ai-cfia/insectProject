# Databricks notebook source
import pandas as pd 
import re 

# COMMAND ----------

def preprocessComments(allComments):
#lower case
    allComments['comments_clean'] = allComments['comments'].apply(lambda x: " ".join(x.lower() for x in x.split()))
    #remove punctuation
    allComments['comments_clean'] = allComments['comments_clean'].str.replace('[^\w\s]','')
    #remove emojis 
    allComments['comments_clean'] = allComments['comments_clean'].apply(lambda x: remove_emoji(x))
    
    allComments['flagged_terms'] = ''
    return allComments 

def flagList(toBeFlaggedList, allComments ):
    flaggedComments=pd.DataFrame(columns=allComments.columns) 
    for i in range(len(toBeFlaggedList)):
        term = toBeFlaggedList[i]
        flaggedComments= flagTerm (term, allComments, flaggedComments)
    return flaggedComments
    
def remove_emoji(text):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags 
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def flagTerm (term, allComments, flaggedComments):
    #term = toBeFlaggedList[i]
    flagged = allComments[allComments.comments_clean.str.contains(term)]
    
    
    repeatedIdx = flagged[flagged.index.isin(flaggedComments.index)].index.values
    #alreadyFlagged = flagged[repeatedIdx]
    try: 
        flaggedComments.loc [repeatedIdx,'flagged_terms'] =  flaggedComments.loc [repeatedIdx,'flagged_terms'] + ', ' + term[3:-3]
    except: 
        a=0
    flagged = flagged[ ~flagged.index.isin(repeatedIdx)]
    flagged['flagged_terms'] = term[3:-3]
    
    
    
    flaggedComments=pd.concat([flaggedComments,flagged])

    return flaggedComments