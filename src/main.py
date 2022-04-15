import subprocess
from tweet_scraper import scrapeTweet

isExecuteScraping = True
isExecuteCleaning = True

if isExecuteScraping:
    scrapeTweet()

if isExecuteCleaning:
    subprocess.call(["pyspark < tweet_info_extractor.py"], shell=True)