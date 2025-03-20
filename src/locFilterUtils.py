# Databricks notebook source


# x,y
# NE=[nelng, nelat ]
# SW= [swlng,  swlat]


#######

"""
#[swlng,nelat]          #X1         #NE

   1                          2

#X2                      #X3         #X4

   3                         4

#SW                    #X5          #[nelng, swlat]



X1 = [(swlng+nelng)/2 , nelat]
X5 = [(swlng+nelng)/2  , swlat]


X3 = [(swlng+nelng)/2, (nelat+swlat)/2]

X4 =[nelng, (nelat+swlat)/2]


X2=[swlng, (nelat+swlat)/2]




boundingbox1= [X2, X1] # bottomleft, topright
boundingbox2= [X3, NE]
boundingbox3= [SW, X3]
boundingbox4= [X5, X4]


"""

import pickle
import time

import pandas as pd

from src.commentsUtils import getNewComments, getObservationsC

nelat = 83.1000000
nelng = -050.7500000
swlat = 41.2833333
swlng = -140.8833333

NE = [nelng, nelat]
SW = [swlng, swlat]

boundingbox = [SW, NE]


def breakBoundingBox(iconic_taxa, datefrom, dateto, boundingbox):
    # run test for number of values
    SW = boundingbox[0]
    NE = boundingbox[1]
    loc = (
        "&nelat="
        + str(NE[1])
        + "&nelng="
        + str(NE[0])
        + "&swlat="
        + str(SW[1])
        + "&swlng="
        + str(SW[0])
    )
    time.sleep(3)
    _, totalnumberofobservations = getObservationsC(
        iconic_taxa, datefrom, dateto, "1", "1", loc
    )  # don't need to download observations
    print(totalnumberofobservations)
    # if totalnumberofobservations!=0:
    #    print(boundingbox)
    if totalnumberofobservations < 10000:
        if totalnumberofobservations == 0:
            return []
        return [boundingbox]

    # calculate coordinates

    swlng = SW[0]
    swlat = SW[1]

    nelng = NE[0]
    nelat = NE[1]

    a = (swlng + nelng) / 2
    b = (nelat + swlat) / 2

    X1 = [a, nelat]
    X5 = [a, swlat]
    X3 = [a, b]
    X4 = [nelng, b]
    X2 = [swlng, b]

    boundingbox1 = [X2, X1]
    boundingbox2 = [X3, NE]
    boundingbox3 = [SW, X3]
    boundingbox4 = [X5, X4]

    # check each boundingbox to have less observations than 10k if not break into smaller boxes
    out = []
    boundingbox1 = breakBoundingBox(iconic_taxa, datefrom, dateto, boundingbox1)
    if len(boundingbox1) != 0:
        out = out + boundingbox1
    boundingbox2 = breakBoundingBox(iconic_taxa, datefrom, dateto, boundingbox2)
    if len(boundingbox2) != 0:
        out = out + boundingbox2
    boundingbox3 = breakBoundingBox(iconic_taxa, datefrom, dateto, boundingbox3)
    if len(boundingbox3) != 0:
        out = out + boundingbox3
    boundingbox4 = breakBoundingBox(iconic_taxa, datefrom, dateto, boundingbox4)
    if len(boundingbox4) != 0:
        out = out + boundingbox4

    return out


def save_obj(obj, name):
    with open("obj/" + name + ".pkl", "wb") as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name):
    with open("obj/" + name + ".pkl", "rb") as f:
        return pickle.load(f)


try:
    boundingBoxOfDate = load_obj("boundingbox.pkl")
except:
    boundingBoxOfDate = {}


for date in newdates:
    # if date not in boundingBoxOfDate.keys():
    boundingBoxOfDate[date] = breakBoundingBox(
        iconic_taxa, datefrom, dateto, boundingbox
    )


save_obj(boundingBoxOfDate, "boundingbox")


commentsAllWindows = pd.DataFrame()
for date in list_of_dates:
    datefrom = date
    dateto = date
    boundingboxes = boundingBoxOfDate[date]

    for loc in boundingboxes:
        comments, commentStatus = getNewComments(
            commentStatus, iconic_taxa, datefrom, dateto, loc
        )
        commentsAllWindows.append(comments)


# look for words here
