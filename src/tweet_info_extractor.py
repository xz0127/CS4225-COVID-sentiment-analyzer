from pyspark.sql import SparkSession
from pathlib import Path
import pathlib
import os
import logging
from py4j.java_gateway import java_import
import subprocess

outputDir = "./../data/cleaned_tweet"

sgDirPath = "../data/scraped_tweet/SG"
usDirPath = "../data/scraped_tweet/US"
inDirPath = "../data/scraped_tweet/IN"
rawDirPath = "../data/scraped_tweet/Raw"

countries = [
    ("US", usDirPath),
    ("IN", inDirPath),
    ("SG", sgDirPath)
]

cleanCrcCmd = "cd ../data && find . -type f -name '*.crc' -exec rm {} +"

# ================ Some initialization =========================================================================

spark = SparkSession.builder.appName("tweet_info_extractor").getOrCreate()
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
sc = spark.sparkContext

subprocess.call(["rm -rf " + outputDir], shell=True)

# ================= Logic to extract sg tweets from raw data ====================================================
sgDataFiles = []
for filename in os.listdir(sgDirPath):
    f = os.path.join(sgDirPath, filename)
    if os.path.isfile(f):
        sgDataFiles.append(f)
rawDataFiles = []
for filename in os.listdir(rawDirPath):
    f = os.path.join(rawDirPath, filename)
    if os.path.isfile(f):
        rawDataFiles.append(f)

def extractSgTweetFromRawData(dataPath, rawDataPath):
    f = open(dataPath)
    count = 0
    for line in f:
        count += 1
    f.close()

    if count < 500:
        rawDataRdd = spark.sparkContext.textFile(rawDataPath)
        sgDataRdd = rawDataRdd.filter(lambda line: "singapore" in line.lower() or "SG" in line)
        sgDataLst = sgDataRdd.collect()

        dataFile = open(dataPath, 'a')
        for line in sgDataLst:
            dataFile.write(line + "\n")
        dataFile.close()

def checkDateEqualFromFilename(filename1, filename2):
    nameList1 = filename1.split('_')
    nameList2 = filename2.split('_')
    return nameList1[-1] == nameList2[-1] and nameList1[-2] == nameList2[-2] and nameList1[-3] == nameList2[-3]

# Driver logic to extract sg tweets from raw tweets
for i in range(len(sgDataFiles)):
    if i >= len(rawDataFiles):
        break
    
    sgDataPath = sgDataFiles[i]
    rawDataPath = rawDataFiles[i]

    if not checkDateEqualFromFilename(sgDataPath, rawDataPath):
        logging.warning('Raw data file does not match date of sg data file for ' + sgDataPath)
        continue
    
    extractSgTweetFromRawData(sgDataPath, rawDataPath)

# ==================== END ==========================================================================================

# ==================== Logic to extract key fields from raw json ====================================================
def getDateStrFromPath(path):
    strs = path.split("_")
    year = strs[-3]
    month = strs[-2]
    day = strs[-1].split(".")[0]
    return year + "_" + month + "_" + day

def extractKeyfieldsFromPath(dataPath, countryCode, countryOutputDir):
    dateStr = getDateStrFromPath(dataPath)
    outputName = countryCode + "_" + dateStr + "_cleaned.json"
    outputPath = countryOutputDir + "/" + outputName
    outputPathTemp = outputPath + "-temp"

    df = spark.read.json(dataPath)
    res = df.select("id", "conversationId", "content", "coordinates.latitude", "coordinates.longitude", 
        "place.country", "place.countryCode", "place.fullName", "user.location", "date").toDF(
            "id",
            "conversationId",
            "content",
            "latitude",
            "longitude",
            "country",
            "countryCode",
            "locationName",
            "userLocation",
            "date"
        ).distinct()
    
    res.coalesce(1).write.json(outputPathTemp)

    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    fl = fs.globStatus(sc._jvm.Path(outputPathTemp + '/part*'))[0].getPath().getName()
    fs.rename(sc._jvm.Path(outputPathTemp + '/' + fl), sc._jvm.Path(outputPath))
    fs.delete(sc._jvm.Path(outputPathTemp), True)


for country in countries:
    countryCode = country[0]
    countryInputDir = country[1]
    countryOutputDir = outputDir + "/" + countryCode
    pathlib.Path(countryOutputDir).mkdir(parents=True, exist_ok=True)

    for filename in os.listdir(countryInputDir):
        f = os.path.join(countryInputDir, filename)
        if os.path.isfile(f):
            extractKeyfieldsFromPath(f, countryCode, countryOutputDir)

subprocess.call([cleanCrcCmd], shell=True)