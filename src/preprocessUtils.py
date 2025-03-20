# Databricks notebook source
from geopy.geocoders import Nominatim

def getLocation_perObservation(coord):
    locator = Nominatim(user_agent='inectsiNatApp',timeout=None)
    
    coordinates=str(coord[1]) +', ' + str(coord[0])
    
    location = locator.reverse(coordinates)
    if (location is not None):
      location = location.raw 
    location = location['address']
            
    country=location['country_code']
    #print("Country")
    #print(country)
    try:
        province=location['state']
    except: 
        province="" 
        print("no province" )
        #print(country) 
        
    
    
    try:
        city=location['city']
    except:   
        try:
            city = location ['town']
        except: 
            
            try: 
                city = location ['county']
            except:
                
                city = '' 
            
                print(coordinates)
                #comement out / for testing purposes 
                print("NOCITY")
                print(country)
    #print(city)
    #print(province)
    
    return city,province,country



'''
does not work / location can be written in a lot of different ways [cannot determine how many to create a robust implementation] (two examples below)
"Prince Edward Island, Resort Mun. Stan.B.-Hope R.-Bayv.-Cavend.-N.Rust., PE, CA"
"Caledonia, NS B0T 1B0, Canada"
def cleanLocString(locString):
    locString = locString.split(", ")
    city, province, country = locString[0], locString[1], locString[2]
    province = province.split(" ")
    province = province [0]
    return city, province, country 
'''


'''
# reverse_geocoder sometimes provides the wrong city 

import reverse_geocoder as rg 
def getLocation_perObservation(coord):
    coord=[coord[1],coord[0]]
    result = rg.search(coord) 
    result=result[0]
    city=result['name']
    province=result['admin1']
    country=result['cc']
    return city,province,country
'''



   

def FilterByLocaiton(df):
    df['City']=''
    df['Province']=''
    todrop=[]
   
    for i in df.index: 
        coord=df[df.index==i]['coordinates'].values[0]
        city,province,country=getLocation_perObservation(coord)
        #if country!='CA': 
        if country!='ca':
            if country!='us':  # new reverse geocoder uses a different format   
               todrop.append(i)
        
        df.loc[df.index==i,'City']=city
        df.loc[df.index==i,'Province']=province
        
        
        
    
    #uncomment below to filter 
    df= df.drop(todrop) #remove observations not in Canada 
    df.reset_index(inplace=True,drop=True) 
    df= df.drop(['coordinates'],axis=1) 
    return df 

def SampleImg(df):
    for i in df.index: 
        image = None
        images=df[df.index==i]['Sample_Img'].values
        if len(images[0])>0:
            image=images[0][0] 
        
        df.loc[df.index==i,'Sample_Img']=image
    return df

def seperateByTaxa(dfRegulated,upperTaxa):
    col='upper taxa'
    dfInsecta=dfRegulated[dfRegulated[col]==upperTaxa[0]].reset_index(drop=True)
    dfMollusca=dfRegulated[dfRegulated[col]==upperTaxa[1]].reset_index(drop=True)
    dfPlantae=dfRegulated[dfRegulated[col]==upperTaxa[2]].reset_index(drop=True)
    dfFungi=dfRegulated[dfRegulated[col]==upperTaxa[3]].reset_index(drop=True)
    dfChromista=dfRegulated[dfRegulated[col]==upperTaxa[4]].reset_index(drop=True)
    
    
    
    dfInsecta=dfInsecta.drop(columns=['upper taxa'])
    dfMollusca=dfMollusca.drop(columns=['upper taxa'])
    dfPlantae=dfPlantae.drop(columns=['upper taxa'])
    dfFungi=dfFungi.drop(columns=['upper taxa'])
    dfChromista=dfChromista.drop(columns=['upper taxa'])
    
    
    dfOthers= dfRegulated[~dfRegulated[col].isin(upperTaxa)]
    if len(dfOthers!=0): 
        dfOthers=dfOthers.reset_index(drop=True)
        dfOthers=dfOthers.drop(columns=['upper taxa'])
    
    return dfInsecta, dfMollusca, dfPlantae, dfFungi, dfChromista, dfOthers

def sortByProvince(dfRegulated): 
    dfRegulated=dfRegulated.sort_values(by=['Province'])
    return dfRegulated

# COMMAND ----------

