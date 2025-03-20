
# COMMAND ----------

# MAGIC %run ./preprocessUtils

# COMMAND ----------

# MAGIC %run ./getListOfSpeciesUtils

# COMMAND ----------

# MAGIC %run ./modelsPrep

# COMMAND ----------

# MAGIC %run ./apiUtils

# COMMAND ----------

# MAGIC %run ./emailUtils

# COMMAND ----------
import pandas as pd

from src.apiUtils import getObsv_allPages
from src.dates import get_yesterday
from src.emailUtils import send_email
from src.getListOfSpeciesUtils import getListOfIDs
from src.modelsPrep import getModels, predict
from src.preprocessUtils import (
    FilterByLocaiton,
    SampleImg,
    seperateByTaxa,
    sortByProvince,
)

test = 0
receiver_email = []  # TODO
############################## to_change

MLcolumn = "ai_visionVerification"
############################## fixed
datefrom = get_yesterday()
dateto = get_yesterday()


Title = "Observations of Invasive Species Submitted On " + datefrom

geobound = 1
projectID = 91863
sender_email = "earlywarning.invasive@gmail.com"
columns_in_out = [
    "Quality",
    "Name1",
    "Name2",
    "Obsvn_URL",
    "Sample_Img",
    "coordinates",
    "posted_on",
    "observed_on",
    "user_login",
    "Obsvn_ID",
    "upper taxa",
    "upper_taxa_id",
]  # "comments"]
upperTaxa = ["Insecta", "Mollusca", "Plantae", "Fungi", "Chromista"]
ordered = [
    "Quality",
    "Name1",
    "Name2",
    "City",
    "Province",
    "Obsvn_URL",
    "Sample_Img",
    "posted_on",
    "observed_on",
    "user_login",
    "Obsvn_ID",
    "upper taxa",
    "ai_visionVerification",
]
ordered_us = [
    "Quality",
    "Name1",
    "Name2",
    "City",
    "Province",
    "Obsvn_URL",
    "Sample_Img",
    "posted_on",
    "observed_on",
    "user_login",
    "Obsvn_ID",
    "upper taxa",
]
page = 1
############################## Define ML Engine Parameters (repeat for m2 )
models = getModels()
############################## Get list of species from project
# regulated_species=getListOfSpecies(projectID)
regulated_ids = getListOfIDs(projectID)
print(regulated_ids)
if regulated_ids == []:
    Title = Title + " error: website is temporary disabled due to maintenance"

"""
324726-Lycorma-delicatula 
Box Tree moth (Cydalima perspectalis),325295-Cydalima-perspectalis
Strawberry blossom Weevil (Anthonomus rubi), and 471260-Anthonomus-rubi
Elm Zigzag Sawfly (Aproceros leucopoda). 497733-Aproceros-leucopoda
]
"""
regulated_ids_us = ["324726", "325295", "471260", "497733"]
geobound_us = 2

############################## Machine Learning Model test  (Remove)

if test == 1:  # remove
    regulated_species = [
        ["Monochamus scutellatus", "Insecta"],
        ["Helix lucorum", "Plantae"],
        ["Anoplophora glabripennis", "Insecta"],
        ["Anoplophora chinensis", "Insecta"],
    ]  # Turkish Snail, asian longhorned and citrus longhorned beetle
    regulated_ids = ["82043", "128525", "199326"]  # second '47115'  #
    regulated_ids = ["128525", "199326"]
    datefrom = "2019-01-01"
    dateto = "2020-10-15"

# ueryTargets=[ '82043',  '128525', '199326']

############################## Get invasive observations (all pages) in a period
invasiveObservations = getObsv_allPages(
    regulated_ids, datefrom, dateto, geobound, page, columns_in_out
)
invasiveObservations_us = getObsv_allPages(
    regulated_ids_us, datefrom, dateto, geobound_us, page, columns_in_out
)

invasiveObservations[MLcolumn] = ""
############################## noninvasive whitespottedSawyerBeetle gets flagged out (they are specifying its upper taxa) so remove it
nonIvasiveRows = invasiveObservations.Name2.str.contains("Monochamus scutellatus")
invasiveObservations = invasiveObservations.drop(
    invasiveObservations[nonIvasiveRows].index
).reset_index(drop=True)
############################## Get nonInvasive Lookalikes observations (all pages) in a period
initial = "non_invasive"
print("model in models forloop - noninvasive")
for model in models:
    for specie in model["nonInvasive"]:
        if (
            test == 1
        ):  # remove (so many observations of white spotted so we need to remove them for the ip address not to be banned)
            datefrom = "2020-10-01"
            dateto = "2020-10-15"
        specie_id = [specie["id"]]
        nonInvasiveLookAlikes_i = getObsv_allPages(
            specie_id, datefrom, dateto, geobound, page, columns_in_out
        )

        # run through model if invasive add and assign value in column

        image_sets = nonInvasiveLookAlikes_i.Sample_Img
        predictions = predict(image_sets, model["model"], initial)
        nonInvasiveLookAlikes_i[MLcolumn] = predictions

        nonInvasiveLookAlikes_i = nonInvasiveLookAlikes_i[
            nonInvasiveLookAlikes_i[MLcolumn] == "invasive"
        ].reset_index(drop=True)
        # append invasive to output data (tobedone 1)
        # uncomment to include over the non invasive samples
        invasiveObservations = pd.concat(
            [invasiveObservations, nonInvasiveLookAlikes_i]
        )
invasiveObservations = invasiveObservations.reset_index(drop=True)
############################## Get nonInvasive Lookalikes observations (all pages) in a period
# apply to invasive counterparts and do the opposite (tobedone 2)
initial = "invasive"
print("model in models forloop - invasive")
for model in models:
    for specie in model["invasive"]:
        image_sets = invasiveObservations[
            invasiveObservations.Name1 == specie
        ].Sample_Img.values
        predictions = predict(image_sets, model["model"], initial)
        invasiveObservations.loc[invasiveObservations.Name1 == specie, MLcolumn] = (
            predictions
        )
##############################
invasiveObservations = invasiveObservations.drop_duplicates(
    subset=["Obsvn_ID"]
).reset_index(drop=True)
invasiveObservations_us = invasiveObservations_us.drop_duplicates(
    subset=["Obsvn_ID"]
).reset_index(drop=True)
##############################
dfRegulated = FilterByLocaiton(invasiveObservations)
print("dfRegulated.info()-1", dfRegulated.info())
dfRegulated_us = FilterByLocaiton(invasiveObservations_us)
############################## Modification 1: Change order of columns (recieved by email on Jan 12th)
dfRegulated = dfRegulated[ordered]
dfRegulated_us = dfRegulated_us[ordered_us]
############################## Modification 2: Sort by Province (recieved by email on Jan 12th)
dfRegulated = sortByProvince(dfRegulated)
############################## keep only one image
dfRegulated = SampleImg(dfRegulated)
dfRegulated_us = SampleImg(dfRegulated_us)
############################## Modification 3: Seperate tables (recieved by email on Jan 12th)
dfInsecta, dfMollusca, dfPlantae, dfFungi, dfChromista, dfOthers = seperateByTaxa(
    dfRegulated, upperTaxa
)
dfInsecta_us, dfMollusca_us, dfPlantae_us, dfFungi_us, dfChromista_us, dfOthers_us = (
    seperateByTaxa(dfRegulated_us, upperTaxa)
)
##############################
# html=pandas_tohtml(dfRegulated)
print("dfRegulated_us.info()-2", dfRegulated_us.info())
print("dfInsecta.head()", dfInsecta.head())

for remail in receiver_email:
    print(remail)
    send_email(
        remail,
        dfInsecta,
        dfPlantae,
        dfMollusca,
        dfFungi,
        dfChromista,
        dfOthers,
        dfInsecta_us,
        dfMollusca_us,
        dfPlantae_us,
        dfFungi_us,
        dfChromista_us,
        dfOthers_us,
        Title,
    )
