# Databricks notebook source
# MAGIC %run ./apiUtils

# COMMAND ----------

import pprint  
import urllib, json
from datetime import date
from datetime import datetime, timedelta
import pandas as pd
import time
import urllib.request # remove if 



def getProjectUrl(projectID):
    url='https://api.inaturalist.org/v1/projects?id='+str(projectID)
    url=url+'&rule_details=true'
    return url 

def getJSON(url):
    JSON = ''
    try:
        response = urllib.request.urlopen(url)
        JSON = json.loads(response.read())
    except Exception as e:
        print(e, 'failed to scrap from '+url)
    
    return JSON
def getTaxa(rule):
    if rule['operator']!="in_taxon?":
        return "null", "null"
    try:
        taxon=rule['taxon'] 
        
        #to not include genus 
        #if taxon['rank']=="species" or taxon['rank'] == "subspecies":
        #    return taxon['name']
        #else: 
        #    return 'null'
        return taxon['name'], taxon['iconic_taxon_name']
    except: 
        print(rule.keys())
        return 'null', 'null'
def getListOfSpecies(projectID):
    url=getProjectUrl(projectID)
    JSON=getJSON(url)['results'][0]
    rules=JSON['project_observation_rules']
    listOfSpecies=[]
    #upper_taxa_categories=[]

    for rule in rules:
        taxa, upperTaxa=getTaxa(rule)
        
        if taxa!='null':


            listOfSpecies.append([taxa,upperTaxa])
    return listOfSpecies


def getID(rule):
    if rule['operator']!="in_taxon?":
        return "null" 
    try:
        taxon=rule['taxon'] 
        
        #to not include genus 
        #if taxon['rank']=="species" or taxon['rank'] == "subspecies":
        #    return taxon['name']
        #else: 
        #    return 'null'
        return taxon['id']
    except: 
        print(rule.keys())
        return 'null'



def getListOfIDs(projectID):
    url=getProjectUrl(projectID)
    
    JSON=getJSON(url)
    listOfIDs=[]
    if JSON != '':
        JSON = JSON['results'][0]
        rules=JSON['project_observation_rules']
        #upper_taxa_categories=[]
        for rule in rules:
            idOfRule=getID(rule)
            
            if idOfRule!='null':
                #if taxa=='Helix': 
                #    upperTaxa='Plantae' 
                listOfIDs.append(str(idOfRule))
                #upper_taxa_categories.append(upperTaxa)
    
    #upper_taxa_categories=set(upper_taxa_categories)
    #upper_taxa_categories=list(upper_taxa_categories) 
    #return listOfSpecies, upper_taxa_categories 
    return listOfIDs

# COMMAND ----------

