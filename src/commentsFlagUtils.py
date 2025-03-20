# Databricks notebook source
import re

import pandas as pd

# COMMAND ----------


def preprocessComments(allComments):
    # lower case
    allComments["comments_clean"] = allComments["comments"].apply(
        lambda x: " ".join(x.lower() for x in x.split())
    )
    # remove punctuation
    allComments["comments_clean"] = allComments["comments_clean"].str.replace(
        "[^\w\s]", ""
    )
    # remove emojis
    allComments["comments_clean"] = allComments["comments_clean"].apply(
        lambda x: remove_emoji(x)
    )

    allComments["flagged_terms"] = ""
    return allComments


def flagList(toBeFlaggedList, allComments):
    flaggedComments = pd.DataFrame(columns=allComments.columns)
    for i in range(len(toBeFlaggedList)):
        term = toBeFlaggedList[i]
        flaggedComments = flagTerm(term, allComments, flaggedComments)
    return flaggedComments


def remove_emoji(text):
    emoji_pattern = re.compile(
        "["
        "\U0001f600-\U0001f64f"  # emoticons
        "\U0001f300-\U0001f5ff"  # symbols & pictographs
        "\U0001f680-\U0001f6ff"  # transport & map symbols
        "\U0001f1e0-\U0001f1ff"  # flags
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r"", text)


def flagTerm(term, allComments, flaggedComments):
    # term = toBeFlaggedList[i]
    flagged = allComments[allComments.comments_clean.str.contains(term)]

    repeatedIdx = flagged[flagged.index.isin(flaggedComments.index)].index.values
    # alreadyFlagged = flagged[repeatedIdx]
    try:
        flaggedComments.loc[repeatedIdx, "flagged_terms"] = (
            flaggedComments.loc[repeatedIdx, "flagged_terms"] + ", " + term[3:-3]
        )
    except:
        a = 0
    flagged = flagged[~flagged.index.isin(repeatedIdx)]
    flagged["flagged_terms"] = term[3:-3]

    flaggedComments = pd.concat([flaggedComments, flagged])

    return flaggedComments
