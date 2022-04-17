from datetime import date, timedelta
from pathlib import Path
import pathlib
import subprocess


def scrapeTweet():

    # Global variables
    outputDir = "./../data/scraped_tweet"

    isRawDataNeeded = False
    isRetweetNeeded = False
    isReplyNeeded = False

    keywords = [
        "covid",
        "corona",
        "virus",
        "Wuhan",
        "Covid",
        "Covid-19",
        "Coronavirus",
        "Mask",
        "Quarantine",
        "ICU",
        "Vaccine",
        "booster",
        "lockdown"
        "circuit-breaker"
        "pandemic"
    ]

    keywordsSG = [
        "(Singapore OR SG) AND "
        "(covid",
        "corona",
        "virus",
        "Wuhan",
        "Covid",
        "Covid-19",
        "Coronavirus",
        "Mask",
        "Quarantine",
        "ICU",
        "Vaccine",
        "booster",
        "lockdown"
        "circuit-breaker"
        "pandemic)"
    ]

    countries = [
        "US",
        "IN",
        "SG"
    ]

    lang = "en"

    startDate = date(2020, 2, 1)
    endDate = date(2020, 2, 20)

    # Some initialization
    subprocess.call(["rm -rf " + outputDir], shell=True)

    # main scraping logic
    for country in countries:

        outputPath = outputDir + "/" + country
        currDate = startDate

        path = pathlib.Path(outputPath)
        path.mkdir(parents=True, exist_ok=True)

        while currDate <= endDate:
            commandBody = "snscrape --max-results 500 --jsonl twitter-search \""

            # append keywords filter to the command body
            if country == "SG":
                for i, keyword in enumerate(keywordsSG):
                    if i > 0:
                        commandBody += " OR "
                    commandBody += keyword
            else:
                for i, keyword in enumerate(keywords):
                    if i > 0:
                        commandBody += " OR "
                    commandBody += keyword

            # append location filter to the command body
            if country != "SG":
                commandBody += " near:" + country
            # if country == "SG":
            #     commandBody += " within:30mi"

            # append language filter to the command body
            commandBody += " lang:" + lang

            # append retweet filter to the command body
            if not isRetweetNeeded:
                commandBody += " -is:retweet"

            # append reply filter to the command body
            if not isReplyNeeded:
                commandBody += " -is:reply"

            # generate and append date filter to the command body
            nextDate = currDate + timedelta(days=1)
            currDateStr = currDate.strftime('%Y-%m-%d')
            nextDateStr = nextDate.strftime('%Y-%m-%d')
            commandBody += " since:" + currDateStr + " until:" + nextDateStr

            # generate and append output file name to the command body
            fileNameStr = "/" + country + "_" + \
                currDateStr.replace("-", "_") + ".json"
            commandBody += "\" > " + outputPath + fileNameStr

            # call command and move on to next iteration
            subprocess.call([commandBody], shell=True)
            currDate = nextDate
            print(commandBody)

    # main scraping logic for raw results without location specification

    if isRawDataNeeded:
        outputPath = outputDir + "/Raw"
        path = pathlib.Path(outputPath)
        path.mkdir(parents=True, exist_ok=True)
        currDate = startDate

        while currDate <= endDate:
            commandBody = "snscrape --max-results 5000 --jsonl twitter-search \""

            # append keywords filter to the command body
            for i, keyword in enumerate(keywords):
                if i > 0:
                    commandBody += " OR "
                commandBody += keyword

            # append language filter to the command body
            commandBody += " lang:" + lang

            # append retweet filter to the command body
            if not isRetweetNeeded:
                commandBody += " -is:retweet"

            # append reply filter to the command body
            if not isReplyNeeded:
                commandBody += " -is:reply"

            # generate and append date filter to the command body
            nextDate = currDate + timedelta(days=1)
            currDateStr = currDate.strftime('%Y-%m-%d')
            nextDateStr = nextDate.strftime('%Y-%m-%d')
            commandBody += " since:" + currDateStr + " until:" + nextDateStr

            # generate and append output file name to the command body
            fileNameStr = "/Raw_" + currDateStr.replace("-", "_") + ".json"
            commandBody += "\" > " + outputPath + fileNameStr

            # call command and move on to next iteration
            subprocess.call([commandBody], shell=True)
            currDate = nextDate
            print(commandBody)