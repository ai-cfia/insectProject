# Databricks notebook source
# MAGIC %run ./apiUtils

# COMMAND ----------

# MAGIC %run ./preprocessUtils

# COMMAND ----------

import reverse_geocoder as rg 
import pprint  
import urllib, json
from datetime import date
from datetime import datetime, timedelta
import pandas as pd
import time
import urllib.request
#from preprocessUtils import getLocation_perObservation
import math
import time 
from datetime import datetime, timedelta
import pickle 
import re 
#from apiUtils import getYesterday
#from apiUtils import createlinktofullimageVer2

# COMMAND ----------

# breaking the interval is insufficient as for the last year we have (22+) days with more than 10k observations, need another filter
'''
def breakIntoSmallerWindows(iconic_taxa ,datefrom ,dateto):  # check if in interval satisfies window limit, if not break into halves then recusrsively check if the halves satisfy the limit
    url=getQueryC(iconic_taxa,datefrom, dateto , '1', '200')
    #time.sleep(3) 
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    totalNumberOfObservations = data['total_results'] 
    
    if totalNumberOfObservations < 10000: 
        return [datefrom,dateto]
    else: 
        interval1, interval2 = breakInterval (datefrom, dateto)
        
        
        interval1= breakIntoSmallerWindows(iconic_taxa, interval1[0], interval1[1])
        interval2= breakIntoSmallerWindows(iconic_taxa, interval2[0], interval2[1])
        
        
        return [interval1, interval2]
def breakInterval (datefrom, dateto):  # breaks time interval into halves if called
    datefrom_dtobject= datetime.strptime(datefrom, '%Y-%m-%d')
    dateto_dtobject= datetime.strptime(dateto, '%Y-%m-%d')
    time_delta= dateto_dtobject-datefrom_dtobject
    interval1 = [str(datefrom_dtobject)[:10], str(datefrom_dtobject+time_delta/2 - timedelta(days=1))[:10] ]
    interval2 = [str(datefrom_dtobject+time_delta/2 )[:10], str(dateto_dtobject)[:10]]
    return interval1, interval2 
'''
def breakBoundingBox(iconic_taxa ,datefrom, dateto, boundingbox): 
    # run test for number of values 
    SW = boundingbox[0]
    NE = boundingbox[1]
    loc = '&nelat='+ str(NE[1]) +'&nelng=' + str(NE[0]) +'&swlat=' + str(SW[1]) + '&swlng='+ str(SW[0]) 
    time.sleep(3) 
    _, totalnumberofobservations = getObservationsC(iconic_taxa ,datefrom, dateto , '1', '1', loc) # don't need to download observations
    print(totalnumberofobservations)
    #if totalnumberofobservations!=0: 
    #    print(boundingbox)
    if totalnumberofobservations < 10000: 
        if totalnumberofobservations==0:
            return []
        return [boundingbox]
    
    # calculate coordinates  
    
    swlng=SW[0]
    swlat=SW[1]
    
    nelng=NE[0]
    nelat=NE[1]
    
    
    a=(swlng+nelng)/2
    b=(nelat+swlat)/2
    
    X1 = [a , nelat]
    X5 = [a  , swlat]       
    X3 = [a, b]
    X4 =[nelng, b]
    X2=[swlng, b]
    
    
             
    boundingbox1= [X2, X1] 
    boundingbox2= [X3, NE]
    boundingbox3= [SW, X3]
    boundingbox4= [X5, X4]
    
    # check each boundingbox to have less observations than 10k if not break into smaller boxes
    out=[]
    boundingbox1=breakBoundingBox(iconic_taxa ,datefrom, dateto,boundingbox1)
    if len(boundingbox1)!=0: 
        out=out+boundingbox1
    boundingbox2=breakBoundingBox(iconic_taxa ,datefrom, dateto,boundingbox2)
    if len(boundingbox2)!=0: 
        out=out+boundingbox2
    boundingbox3=breakBoundingBox(iconic_taxa ,datefrom, dateto,boundingbox3)
    if len(boundingbox3)!=0: 
        out=out+boundingbox3
    boundingbox4=breakBoundingBox(iconic_taxa ,datefrom, dateto,boundingbox4)
    if len(boundingbox4)!=0: 
        out=out+boundingbox4
        
        
    return out


def getObsvsInfo (flaggedComments,cols):
    flaggedComments = flaggedComments.reset_index(drop=True)
    flaggedComments[cols]= ''
    ids=flaggedComments.id.values
    s=0
    repeat = 1 
    while repeat : 
    
        for i in range(s, len(ids)):
            ID = ids[i]
            time.sleep(3) 
            observation = getObservation_byObsvID(ID,cols)
            try:
                flaggedComments.loc[flaggedComments.id==ID, cols]=[observation]
            except:
                s=i
                break
        repeat = 0 
    return flaggedComments


def getComments(maxCountTrials, dates, queryPairs, commentStatus, iconic_taxa):
    allComments=pd.DataFrame() 
    queryIsComplete = False 
    s1=0
    s2=0 
    countTrials = 0 
    while (not queryIsComplete) and (countTrials<maxCountTrials): # api will throttle and the code will crash 
        i, j, allComments, commentStatus = runQuery(s1,s2,dates,queryPairs,allComments, commentStatus, iconic_taxa )
        s1, s2 = i, j
        
        if s1 == len(dates ):
            queryIsComplete = True 
        
        '''
        if s1 == (len(dates)-1):
            date=dates[i]
            boundingboxes = queryPairs[date] 
            if s2 == len (boundingboxes) - 1: 
                queryIsComplete = True 
        '''
        
    return queryIsComplete, allComments, commentStatus



def runQuery(s1,s2,dates,queryPairs,allComments, commentStatus, iconic_taxa ):
    for i in range(s1, len(dates)):
        
        date = dates [i]
        print(date)
        boundingboxes = queryPairs[date] 
        for j in range(s1, len(boundingboxes)): 
            box = boundingboxes[j]
            #allComments, commentStatus = runQuery_Datei_Boxj (allComments, commentStatus, iconic_taxa ,  box, date)
            try: 
                allComments, commentStatus = runQuery_Datei_Boxj (allComments, commentStatus, iconic_taxa ,  box, date)
            except:
                return i, j, allComments, commentStatus
            
    # if it gets here then all queries concluded successfully  
    i=len(dates)
    return  i, 0, allComments, commentStatus
    

        
        
        
def runQuery_Datei_Boxj (allComments, commentStatus, iconic_taxa ,  box, date): 
    datefrom = date 
    dateto = date
    SW = box[0]
    NE = box[1]
    loc = '&nelat='+ str(NE[1]) +'&nelng=' + str(NE[0]) +'&swlat=' + str(SW[1]) + '&swlng='+ str(SW[0]) 
    newComments, commentStatus= getNewComments ( commentStatus,iconic_taxa ,datefrom, dateto, loc)
    allComments=allComments.append(newComments).reset_index(drop=True)
    return allComments, commentStatus 



def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open( name + '.pkl', 'rb') as f:
        return pickle.load(f)





def computeBoundingBoxes(iconic_taxa, boundingbox, queryPairs_dateNew):
    # queryPairs_dateNew: list of dates to compute this for 
    boundingBoxOfDate={}
    for date in queryPairs_dateNew: 
            #print(date)
            print(date)
            datefrom = date 
            dateto = date
            boundingBoxOfDate[date] = breakBoundingBox(iconic_taxa ,datefrom, dateto, boundingbox)
    return boundingBoxOfDate



def getQueryPairs(queryPairs, new_dates, boundingBoxOfDate):
    '''
    for date in new_dates: 
        boundingboxes=boundingBoxOfDate[date]
        for box in boundingboxes: 
            newPair = [date, box]
            queryPairs.append(newPair)
    return queryPairs
    ''' 
    
    for date in new_dates: 
        queryPairs[date]=boundingBoxOfDate[date]
    
    return queryPairs


def getDates(interval, queryPairs):
    
    yesterday = getYesterday() 
    yesterday_datetime = datetime.strptime(yesterday, '%Y-%m-%d')
    one = datetime.strptime('2020-01-01', '%Y-%m-%d')
    two = datetime.strptime('2020-01-02', '%Y-%m-%d')
    step = two - one 
    
    dates = []
    queryPairs_dateNew = []
    for i in reversed(range (interval+1)): 
        date = yesterday_datetime - step * i
        date = str(date)
        date = date [:10]
        dates.append(date) 
        
        # calculate bounding boxes for new dates 
        if date not in queryPairs.keys(): 
            queryPairs_dateNew.append(date)
            
    return dates, queryPairs_dateNew




def getObservation_byObsvID(ID, cols):
    url = 'https://api.inaturalist.org/v1/observations?id=' + str(ID) + '&order=desc&order_by=created_at'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    observation=data['results']
    
    
    
    
    
    
    summary = getObservationDetails(observation,cols)
    
    
    return summary 

def getObservationDetails(observation,cols):
    observation=observation[0]
    created_date=observation['created_at_details']['date']
    observed_date=observation['time_observed_at']
    username = observation['user']['login']
    #comments=observation   ... removed
    quality_grade=observation['quality_grade']
    try:
        obsName=observation['taxon']['preferred_common_name']
    except:
        obsName=''
        
    try: 
        obsnameALT=observation['taxon']['name']
    except:
        obsnameALT=''
        
    print(obsName)
    obsURL=observation['uri']
    obsImgURLs=[]
    for photo in observation['photos']:
        urlimg=photo['url']
        obsImgURLs.append(createlinktofullimageVer2(urlimg))
    coord=observation['geojson']['coordinates']
    
    #obsvID=observation['id']
    
    #try: 
    #    upperTaxa=observation['taxon']['iconic_taxon_name']
    #except:
    #    upperTaxa=''
    
    observation_summary=[quality_grade,obsName,obsnameALT,obsURL,obsImgURLs,coord,created_date,observed_date]
    
    
    #observation_summary=pd.DataFrame([observation_summary], columns=cols)
    
    
    return observation_summary


def getQueryC (iconic_taxa , datefrom, dateto , page, per_page, loc):
    start='https://api.inaturalist.org/v1/observations?'
    datefrom ='created_d1=' + datefrom
    dateto  = '&created_d2=' + dateto
    iconic_taxa='&iconic_taxa='+iconic_taxa
    #loc= '&nelat=83.1000000&nelng=-050.7500000&swlat=41.2833333&swlng=-140.8833333' 
    page='&page=' + page
    per_page = '&per_page=' + per_page
    order = '&order=desc&order_by=created_at'
    return start+datefrom+dateto+iconic_taxa+loc+page+per_page+order 



def getObservationsC(iconic_taxa ,datefrom, dateto , page, per_page, loc):
    page=str(page)
    per_page=str(per_page)
    url=getQueryC(iconic_taxa,datefrom, dateto , page, per_page, loc)
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    observations_page_k=data['results']
    totalNumberOfObservations = data['total_results']
    return observations_page_k, totalNumberOfObservations

    


def getNewComments ( commentStatus,iconic_taxa ,datefrom, dateto, loc):
    page = str(1) 
    observations_page_k,totalNumberOfObservations=  getObservationsC(iconic_taxa ,datefrom, dateto , page, str(200),loc)
    comments, ids, commentStatus = getComments_page_k (observations_page_k, commentStatus)
    numberOfPages= math.ceil(totalNumberOfObservations/200)
    
    for page_number in range (2,numberOfPages+1): 
        time.sleep(3) 
        page= str(page_number)
        observations_page_k, _=  getObservationsC(iconic_taxa ,datefrom, dateto , page, str(200), loc)
        comments_k, ids_k, commentStatus = getComments_page_k (observations_page_k, commentStatus)
        
        comments=comments + comments_k
        ids= ids+ ids_k
        
        
        
    

    data_transposed = zip(comments,ids)
    newComments = pd.DataFrame (data_transposed, columns=['comments','id'])
    return newComments, commentStatus



def getComments_page_k (observations_page_k, commentStatus): 
    comments=[] 
    ids=[]
     
    for observation in observations_page_k:
        numberOfComments= observation['comments_count']
        if numberOfComments==0: # no need to investigate further
            continue 
        observation_id = observation['id']
        
        if numberOfComments == commentStatus [commentStatus['observation_id']==observation_id].comment_count.values: # no need to investigate further 
            continue 

        
        newObservation = observation_id not in commentStatus['observation_id']
            
        
        # if we get here then there are new comments to be investigated (either all or a subset)
        comments_of_observation, commentStatus = getComments_observation (observation, observation_id, numberOfComments, newObservation, commentStatus) 
        
        if len(comments_of_observation)!=0:  # only add comments of interest (i.e., in Canada): country check done inside
            for comment in comments_of_observation:
                
                comments.append(comment)
                ids.append(observation_id)
        
    
    return comments, ids, commentStatus
        
def getComments_observation (observation, observation_id, numberOfComments, newObservation, commentStatus)  : 
    comments = observation['comments']
    if not newObservation: 
        
        if commentStatus [commentStatus['observation_id']==observation_id].country.values !='': # if country is not CA 
                return [], commentStatus
    
        
        # get new comments only 
        previousCommentCount = commentStatus [commentStatus['observation_id']==observation_id].comment_count.values
        newComments = numberOfComments - previousCommentCount 
        comments = comments [:newComments]
        
        # update number of comments to reflect new comments
        commentStatus.loc[commentStatus.observation_id==observation_id, 'comment_count'] =numberOfComments
        
        
    else: 
        
        
        coord=observation['geojson']['coordinates']
        country = getCountry(coord)
        # update status 
        commentStatusUpdate = { "observation_id" : observation_id , "comment_count" : numberOfComments, "country": country}
        commentStatus = commentStatus.append (commentStatusUpdate, ignore_index=True)
        
        if country!='': # if country is not CA 
                return [], commentStatus
    
    
    comments = extractCommentsText_perObservation(comments)
    
    
    
    
    return comments, commentStatus
        # get comments 
        
     
def extractCommentsText_perObservation(comments):
    commentsText=[]
    for comment in comments: 
        commentsText.append(comment['body'])
    
    return commentsText
        
        
def getCountry(coord):
    city,province,country=getLocation_perObservation(coord)
    #if country!='CA':  
    if country!='ca':   # new encoding 
        return country 
    else: 
        return '' 
    
    
# ref: https://gist.github.com/slowkow/7a7f61f495e3dbb7e3d767f97bd7304b