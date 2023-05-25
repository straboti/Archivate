
import time
import os
import re
import gzip
import json
from termcolor import colored

#if os.path.exists("./config.json"):
#    settingsFile = json.loads(open("./config.json","r"))
#else:

def info(text):
    print("[INFO]-{}".format(text))
def warn(text):
    print(colored("[WARN]-{}".format(text),"yellow"))
def error(text):
    print(colored("[ERROR]-{}".format(text),"red"))

info("Loading...")
#TODO Implement Archival class

#*This generates the formatted filenames used for archived files
def generateFormattedFileName(fileName,pattern,prefix,suffix,element):
    alteredFileName = ""
    for char in pattern:
        if char == "p":
            alteredFileName += prefix
        elif char == "s":
            alteredFileName += suffix
        elif char == "i":
            alteredFileName += str(element["id"])
        elif char == "n":
            alteredFileName += str(fileName).split(".")[0]
        elif char == "d":
            time_tuple = time.localtime(element["creationTimeStamp"])
            formatted_time = time.strftime('%Y_%m_%d', time_tuple)
            alteredFileName += formatted_time
        elif char == "t":
            time_tuple = time.localtime(element["creationTimeStamp"])
            formatted_time = time.strftime('%H-%M-%S', time_tuple)
            alteredFileName += formatted_time
        else:
            alteredFileName += char
    alteredFileName += "."
    alteredFileName+=str(file).split(".")[-1]    
    return alteredFileName

#*This converts 256M to bytes.
def parseDataSize(original:str):
    suffix = original[-1]
    size = int(original[:-1])
    suffixMults = {
        "B":1,
        "K":1024,
        "M":1024**2,
        "G":1024**3
    }
    return size*(suffixMults[suffix])
#*This converts 10h to secs.
def parseTimeSize(original:str):
    suffix = original[-1]
    size = int(original[:-1])
    suffixMults = {
        "S":1,
        "M":60,
        "H":60*60,
        "d":60*60*24,
        "w":60*60*24*7,
        "m":60*60*24*30,
        "y":60*60*24*365
    }
    return size*(suffixMults[suffix])

#TODO Read this from a file instead
settings = {
    #General
    "sourceFolder":"/home/user/Desktop/dev/archivate/source/", # Full path or relative to working director
    "targetFolder":"/home/user/Desktop/dev/archivate/archived/",
    "keepInSource":False,
    "deleteOfflineFiles":"1S",
    "missingFileBehaviour":"mark",
    "minimumFileSize":"10B", #Inclusive/ >=
    "maximumFileSize":"5G", #Inclusive/ <=
    "fileNameMatches":"\w+\.txt$", #Only archives file if its name (with dot and file format) matches this (regex)
    "delayBetweenChecks":"5S",
    #Naming
    "prefix":"managed",
    "suffix":"",
    "order":"n-d-t",
    #Archiving
    "compressFiles":True,
    #TODO add regex check option for each block
    "timeBlocks": [
        {"endTime":"5S",
         "fileCountDivision":1},
        {"endTime":"10M",
         "fileCountDivision":2},
    ]
}

#* Reads variables from settings file
#Settings: General
SRCFOLDER = str(settings["sourceFolder"])
TGTFOLDER = str(settings["targetFolder"])
KEEPINSRC = bool(settings["keepInSource"])
DELETEOFFLINE = int(parseTimeSize(str(settings["deleteOfflineFiles"])))
MISSINGBEHAVIOUR = str(settings["missingFileBehaviour"])
MINFILESIZE = int(parseDataSize(str(settings["minimumFileSize"]))) #Parses dates
MAXFILESIZE = int(parseDataSize(str(settings["maximumFileSize"])))
FILENAMEMATCHES = str(settings["fileNameMatches"])
CHECKDELAY = int(parseTimeSize(str(settings["delayBetweenChecks"])))

#Settings: Naming
NPREFIX = str(settings["prefix"])
NSUFFIX = str(settings["suffix"])
NAMINGORDER = str(settings["order"])

#Settings: Archiving
COMPRESSFILES = bool(settings["compressFiles"])
TIMEBLOCKSRAW = list(settings["timeBlocks"])

info("Loaded all settings.")

timeBlocksUnsorted = dict()
info("Parsing timeblocks...")
for index,timeblock in enumerate(TIMEBLOCKSRAW):
    #Parse the endTime
    if timeblock["endTime"] == 0:
        endTime = 0
    else:
        endTime = parseTimeSize(timeblock["endTime"])
    fileCountDiv = int(timeblock["fileCountDivision"])
    timeBlocksUnsorted[endTime] = fileCountDiv
tbKeys = list(timeBlocksUnsorted.keys())
tbKeys.sort()
timeBlocks = {i: timeBlocksUnsorted[i] for i in tbKeys}
info("Timeblocks parsed.")

del timeBlocksUnsorted,tbKeys

files = []
maxId = 0
#Sets maxId to the maximum of all the IDs
if len(files) > 0:
    for i in files:
        if i["id"] > maxId:
            maxId = i["id"]
info("Picked up ID.")
filesInSrc = os.listdir(SRCFOLDER)

workFileNames = []
for file in filesInSrc:
    if re.match(FILENAMEMATCHES,str(file)):
        filesize = os.path.getsize(SRCFOLDER+file)
        if filesize >= MINFILESIZE and filesize <= MAXFILESIZE: #Checks if the file size is inside the min/max range.
            workFileNames.append(file)

info("Found {} files in source folder, loading {}.".format(len(filesInSrc),len(workFileNames)))
for file in workFileNames:
    element = {"id":maxId+1,"status":"online","fileSize":os.path.getsize(SRCFOLDER+file),"creationTimeStamp":round(os.path.getctime(SRCFOLDER+file)),"archivalTimeStamp":round(time.time()),"originalName":file}
    maxId+=1
    alteredFileName = generateFormattedFileName(file,NAMINGORDER,NPREFIX,NSUFFIX,element)
    element["newFileName"] = alteredFileName
    #TODO Save this to a file
    files.append(element)
    if COMPRESSFILES:
        element["newFileName"] += ".gz"
        with open(SRCFOLDER+element["originalName"], 'rb') as f_in:
            with gzip.open(TGTFOLDER+alteredFileName+".gz",'wb') as f_out:
                f_out.writelines(f_in)
    else:
        os.rename(SRCFOLDER+file,TGTFOLDER+alteredFileName)

info("STARTING SHARDING LOOP.")
while True:
    filesInDir = os.listdir(TGTFOLDER)
    for id,file in enumerate(files):
        currentTime = round(time.time())
        if file["status"] == "online":
            if not file["newFileName"] in filesInDir: #If file is missing
                warn("'{}' file is missing!".format(file["newFileName"]))
                if MISSINGBEHAVIOUR == "mark":
                    file["status"] = "missing"
                    warn("'{}' was marked as missing.".format(file["newFileName"]))
                elif MISSINGBEHAVIOUR == "remove":
                    files.remove(file)
                    warn("'{}' was purged from the file database.".format(file["newFileName"]))
                elif MISSINGBEHAVIOUR == "placeholder":
                    open(str(file["newFileName"]).replace(".gz",".placeholder"),"x").close()
                    file["status"] = "missing"
                    warn("'{}' was marked as missing and replaced with a placeholder.".format(file["newFileName"]))
            else: #Check sharding rules
                totalDivision = 1
                for timeblockStart in timeBlocks:
                    if currentTime - file["creationTimeStamp"] >= timeblockStart:
                        totalDivision *= timeBlocks[timeblockStart]
                if file["id"] % totalDivision != 0:
                    file["status"] = "offline"
                    info("Putting {} file in offline status.".format(file["newFileName"]))
        elif file["status"] == "offline":
            if currentTime - file["creationTimeStamp"] > DELETEOFFLINE: #Delete file if past deletion time
                os.remove(TGTFOLDER+file["newFileName"])
                file["status"] = "deleted"
                info("Deleted offline file {}".format(file["newFileName"]))
        else:
            pass
        files[id] = file
    time.sleep(CHECKDELAY)