import json
import time
import urllib
import urllib.request
from datetime import datetime, timedelta

import pandas as pd

# DONE
def getQuery(
    queryTarget, page, per_page, date_from, date_to, geo_bound, queryBy="name"
):
    link = "https://api.inaturalist.org/v1/observations?"

    if queryBy == "name":
        queryTarget = "taxon_name=" + queryTarget
    if queryBy == "id":
        queryTarget = "taxon_id=" + queryTarget

    date_from = "&created_d1=" + date_from
    date_to = "&created_d2=" + date_to
    # date_to=''
    bound = (
        "&nelat=83.1000000&nelng=-050.7500000&swlat=41.2833333%20&swlng=-140.8833333"
    )

    bound_us = (
        "&nelat=49.3868470&nelng=-050.9549360&swlat=25.4891560&swlng=-128.8260230"
    )

    page = "&page=" + page
    per_page = "&per_page=" + per_page
    rem = "&order=desc&order_by=created_at"

    time.sleep(3)

    if geo_bound == 1:
        return link + queryTarget + date_from + date_to + bound + page + per_page + rem
    elif geo_bound == 2:
        return (
            link + queryTarget + date_from + date_to + bound_us + page + per_page + rem
        )
    else:
        return link + queryTarget + date_from + date_to + page + per_page + rem



# DONE
def getYesterday():
    date = datetime.now() + timedelta(days=-1)
    if date.day < 10:
        day = "0" + str(date.day)
    else:
        day = str(date.day)
    if date.month < 10:
        month = "0" + str(date.month)
    else:
        month = str(date.month)
    year = str(date.year)

    return year + "-" + month + "-" + day

# DONE
def getObservations(
    queryTarget,
    datefrom=getYesterday(),
    dateto=getYesterday(),
    geobound=1,
    per_page=200,
    page=1,
    queryBy="name",
):
    # geobound: bounding box
    if queryBy == "name":
        queryTarget = queryTarget.replace(" ", "%20")
    page = str(page)
    per_page = str(per_page)
    url = getQuery(queryTarget, page, per_page, datefrom, dateto, geobound, queryBy)
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    observations_page_k = data["results"]
    number_totalObservations = data["total_results"]
    return number_totalObservations, observations_page_k


def createlinktofullimageVer2(url):
    url = url.replace("square", "large")
    return url

# DONE
def getViolations_PerTarget(
    queryTarget,
    datefrom=getYesterday(),
    dateto=getYesterday(),
    geobound=1,
    per_page=200,
    page=1,
    queryBy="name",
):
    number_totalObservations, firstPage_Observations = getObservations(
        queryTarget, datefrom, dateto, geobound, per_page, page, queryBy
    )
    specie_summary = []
    i = 0
    print(number_totalObservations)  # remove
    for observation in firstPage_Observations:
        created_date = observation["created_at_details"]["date"]
        observed_date = observation["time_observed_at"]
        username = observation["user"]["login"]
        # comments=observation   ... removed
        quality_grade = observation["quality_grade"]
        try:
            obsName = observation["taxon"]["preferred_common_name"]
        except Exception:
            obsName = ""

        try:
            obsnameALT = observation["taxon"]["name"]
        except Exception:
            obsnameALT = ""

        print(obsName)
        obsURL = observation["uri"]
        obsImgURLs = []
        for photo in observation["photos"]:
            urlimg = photo["url"]
            obsImgURLs.append(createlinktofullimageVer2(urlimg))
        coord = observation["geojson"]["coordinates"]

        obsvID = observation["id"]

        try:
            upperTaxa = observation["taxon"]["iconic_taxon_name"]
        except:
            upperTaxa = ""

        observation_summary = [
            quality_grade,
            obsName,
            obsnameALT,
            obsURL,
            obsImgURLs,
            coord,
            created_date,
            observed_date,
            username,
            obsvID,
            upperTaxa,
            queryTarget,
        ]
        # print(observation_summary)
        specie_summary.append(observation_summary)
        i = i + 1

    return specie_summary

# DONE
def getViolations(
    queryTargets,
    datefrom=getYesterday(),
    dateto=getYesterday(),
    geobound=1,
    per_page=200,
    page=1,
    queryBy="name",
): # this reutrns a list of observations. And observation being a list of values
    summaryRegulated = []
    # summaryNonRegulated=[]
    for queryTarget in queryTargets:
        # upperTaxa=specie_full[1]
        # specie_full=specie_full[0]
        # specie=["",specie_full]
        summaryRegulated = summaryRegulated + getViolations_PerTarget(
            queryTarget, datefrom, dateto, geobound, per_page, page, queryBy
        )

    # for specie_full in nonRegulated_species:
    #    specie=specie_full
    #    summaryNonRegulated=summaryNonRegulated+getViolations_PerSpecie(specie,datefrom)
    # Summary will take in 5 fields obsName,obsNameALT, obsURL, obsImgURL, Qualitygrade LocationLat, LocationLong
    # return summaryRegulated,summaryNonRegulated
    return summaryRegulated


# DONE
def getObsv_allPages(
    queryTargets, datefrom, dateto, geobound, page, columns_in_out
):  # doesn't work using names
    # sort_flag = 0
    ############################## Do first Run
    regulatedout = getViolations(queryTargets, datefrom, dateto, geobound, queryBy="id")
    data = regulatedout
    RegulatedOutput_total = pd.DataFrame(data, columns=columns_in_out)
    ############################## Check if data exists on a second page for any of the species (unlikely)
    # initialize to be all species
    toRunids = RegulatedOutput_total.upper_taxa_id.value_counts().keys()[
        RegulatedOutput_total.upper_taxa_id.value_counts() == 200
    ]  # get species with remaining observations

    # speciestorerun=[]
    # for specie in toRunNames:
    #    upperTaxaOfSpecie= RegulatedOutput_total[RegulatedOutput_total.Name2==specie]['upper taxa'].unique()[0]
    #    speciestorerun.append([specie,upperTaxaOfSpecie])

    ############################## Do Additional Runs over species that require it
    totalData = data
    while len(toRunids):
        toRunids = list(toRunids)
        page = page + 1
        # sort_flag = 1
        # extract
        regulatedout = getViolations(
            toRunids, datefrom, dateto, geobound, 200, page, queryBy="id"
        )
        data = regulatedout
        RegulatedOutput_total = pd.DataFrame(data, columns=columns_in_out)
        # appendtofinal
        toRunids = RegulatedOutput_total.upper_taxa_id.value_counts().keys()[
            RegulatedOutput_total.upper_taxa_id.value_counts() == 200
        ]
        totalData = totalData + data
        print(page)  # remove
    ############################## Do Additional Runs over species that require it
    RegulatedOutput_total = pd.DataFrame(totalData, columns=columns_in_out)
    ############################## Do run over noninvasive lookalikes
    return RegulatedOutput_total


def main():
    print("ran api")


if __name__ == "__main__":
    main()

# COMMAND ----------
