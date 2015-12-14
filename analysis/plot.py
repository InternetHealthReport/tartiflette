import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pylab as plt
import tools
import rttAnalysis
import pandas as pd
from bson import objectid
import re
from pytz import timezone
import sys
import itertools
from datetime import datetime
from datetime import timedelta
from pytz import timezone
import time
import os
import json
import glob
import numpy as np
from collections import defaultdict
from collections import deque
from scipy import stats
import pymongo
from multiprocessing import Process, JoinableQueue, Manager, Pool
import tools

######## used by child processes
collection = None

def processInit():
    global collection
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    db = client.atlas
    collection = db.traceroute

def computeRtt( (start, end, ip1, ip2) ):
    """Read traceroutes from a cursor. Used for multi-processing.
    """

    tsS = time.time()
    nbRow = 0
    measuredRtt = defaultdict(list)
    inferredRtt = defaultdict(list)
    tsM = time.time()
    cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end},
            "result": { "$elemMatch": { "result": {"$elemMatch": {"from": ip2}} } }} , 
            projection={"timestamp": 1, "result":1, "prb_id":1} , 
            cursor_type=pymongo.cursor.CursorType.EXHAUST,
            batch_size=int(10e6))
    tsM = time.time() - tsM
    for trace in cursor: 
        rttAnalysis.readOneTraceroute(trace, measuredRtt, inferredRtt)
        nbRow += 1
    timeSpent = time.time()-tsS
    # print("Worker %0.1f /sec., dict size: (%s, %s), mongo time: %s, total time: %s"
            # % (float(nbRow)/(timeSpent), len(measuredRtt),len(inferredRtt), tsM, timeSpent))

    return measuredRtt, inferredRtt, nbRow

######## used by child processes

def plotRttEvolution(res):

    mean = []
    ref = []
    refVar = []
    start = np.min(res[3])
    end = np.max(res[3])
    dateRange = [start+timedelta(days=x) for x in range((end-start).days)]

    for d in dateRange:
        ref.append(np.median(res[2][res[3]==d]))
        refVar.append(tools.mad(res[2][res[3]==d]))
        mean.append(np.mean(res[0][res[1]==d]))

    fig = plt.figure()
    # plt.fill_between(dateRange, -1*refVar, refVar, "g", lw=1)
    plt.plot(dateRange, ref, "k", lw=1)
    plt.plot(dateRange, mean, "r", lw=2)
    plt.grid(True)
    fig.autofmt_xdate()
    plt.savefig("rttModel.eps")

    fig = plt.figure()
    plt.plot(res[1], res[0],"x")
    plt.grid(True)
    plt.yscale("log")
    fig.autofmt_xdate() 
    plt.savefig("rttRawData.eps")


def getRttData(ip1,ip2):

    nbProcesses = 6
    binMult = 10
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    # TODO clean this:
    metrics = [np.mean, np.median, tools.mad] 

    expParam = {
            "timeWindow": 60*60, # in seconds 
            "historySize": 24*7,  # 7 days
            "start": datetime(2015, 5, 31, 23, 45, tzinfo=timezone("UTC")), 
            "end":   datetime(2015, 7, 1, 8, 0, tzinfo=timezone("UTC")),
            "msmIDs": range(5001,5027),
            "tau": 3, # multiplied by 1.4826 in outlier detection
            "metrics": str(metrics),
            "minSamples": 50,
            "experimentDate": datetime.now(),
            }

    sampleMeanMeasured = {} 
    sampleMeanInferred = {}
    meanDiffMeasured = {}
    meanDiffInferred = {}

    start = int(time.mktime(expParam["start"].timetuple()))
    end = int(time.mktime(expParam["end"].timetuple()))
    expParam["metrics"] = metrics

    rawRtt = []
    rawRttDate = []
    refRtt = []
    refRttDate = []

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Rtt analysis %s" % datetime.fromtimestamp(currDate))
        tsS = time.time()
        currDatetime = datetime.fromtimestamp(currDate)

        # Get distributions for the current time bin
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (binEdges[i], binEdges[i+1], ip1, ip2) )

        measuredRtt = defaultdict(list)
        inferredRtt = defaultdict(list)
        nbRow = 0 
        rttResults =  pool.imap_unordered(computeRtt, params)
        measuredRtt, inferredRtt, nbRow = rttAnalysis.mergeRttResults(rttResults)

        # Detect oulier values
        for dist, pastMean, pastMeanDiff in [(measuredRtt, sampleMeanMeasured, meanDiffMeasured),
                (inferredRtt, sampleMeanInferred, meanDiffInferred)]:
            rttAnalysis.outlierDetection(dist, pastMean, pastMeanDiff, expParam, None, 
                    currDatetime, None)

        rawRtt.extend(measuredRtt[(ip1,ip2)])
        rawRttDate.extend([currDatetime]*len(measuredRtt[(ip1,ip2)]))
        if (ip1, ip2) in meanDiffMeasured:
            refRtt.extend(meanDiffMeasured[(ip1,ip2)])
            refRttDate.extend([currDatetime]*len(meanDiffMeasured[(ip1,ip2)]))

        timeSpent = (time.time()-tsS)
        sys.stderr.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    
    return (rawRtt, rawRttDate, refRtt, refRttDate)


def nbRttChanges(expIds):

    db = tools.connect_mongo()
    collection = db.rttChanges

    # for expId in expIds:
    cursor = collection.aggregate([
        {"$match": {
            # "expId": objectid.ObjectId(expId),
            "confidence": {"$gt":100}, 
            "ipPair.0": {"$not": re.compile("probe.*")}
            }}, 
            # "ipPair.0": {"$regex": re.compile("probe.*")}}}, 
        {"$group":{"_id": "$timeBin", "count": {"$sum":"$nbSamples"}}},
        {"$sort": {"_id": 1}}
        ])

    df =  pd.DataFrame(list(cursor))
    df["_id"] = pd.to_datetime(df["_id"],utc=True)
    df.set_index("_id")

    fig = plt.figure()
    # plt.plot_date(df["_id"],df["count"],tz=timezone("UTC"))
    plt.plot(df["_id"],df["count"])

    fig.autofmt_xdate()
    plt.grid(True)
    plt.ylabel("Nb. rtt changes in packets")
    plt.show()
        
    plt.savefig("rttChanges.eps")
    
    return df


def nbRouteChanges(expIds):

    db = tools.connect_mongo()
    collection = db.routeChanges

    targets = [
        "193.0.19.33",
        "192.33.4.12",
        "192.5.5.241",
        "199.7.83.42",
        "199.7.91.13",
        "128.63.2.53",
        "192.112.36.4",
        "198.41.0.4",
        "213.133.109.134",
        "192.228.79.201",
        "78.46.48.134",
        "192.58.128.30",
        "192.36.148.17",
        "193.0.14.129",
        "202.12.27.33",
        "192.203.230.10",
        "84.205.83.1"
]

    for dst_ip in targets:
        # for expId in expIds:
        cursor = collection.aggregate([
            {"$match": {"dst_ip": dst_ip}}, #, "corr": {"$lt": 0.05}}},
            # {"$match": {"nbSamples": {"$gt": 500}}},
            # {"$match": {"ip": {"$not": re.compile("probe.*")}}},
            # {"$match": {"expId": objectid.ObjectId(expId), 
                # "ip": {"$not": re.compile("probe.*")}}}, 
                # # "ipPair.0": {"$regex": re.compile("probe.*")}}}, 
            {"$group":{"_id": "$timeBin", "count": {"$sum":1}}},
            {"$sort": {"_id": 1}}
            ])

        # Expand the cursor and construct the DataFrame
        df =  pd.DataFrame(list(cursor))
        df["_id"] = pd.to_datetime(df["_id"],utc=True)
        df.set_index("_id")

        fig = plt.figure()
        # plt.plot_date(df["_id"],df["count"],tz=timezone("UTC"))
        plt.plot(df["_id"],df["count"],label=dst_ip)

        fig.autofmt_xdate()
        plt.grid(True)
        plt.ylabel("Nb. route changes")
        plt.legend()
        plt.savefig("routeChange_%s.eps" % dst_ip)
    

def sampleVsShapiro(results):

    for i, label in [(0,"Measured"), (1,"Inferred")]:
        if resutls[i] is None:
            continue

        plt.figure()

        shapiro = {}
        x = []
        y = []

        for k,v in results[i].iteritems():
            if len(v) > 40:
                x.append(np.mean(results[i+2][k]))
                y.append(stats.shapiro(v)[1])
        
        if i==0:
            plt.plot(x,y,'o')
            plt.title(label)

        plt.xscale("log")
        plt.yscale("log")
        plt.xlabel("Avg. # samples")
        plt.xlabel("Shapiro test")
        # plt.legend()
        plt.grid(True)
        plt.savefig("nbSampleVsShapiro_%s.eps" % label)



def cLTMeanVsStd(results):
    
    # flatten the lists
    rng=None
    for i, label in [ (1, "Inferred"), (0, "Measured")]:
        if resutls[i] is None:
            continue

        data = results[i].values()
        #filtering
        data = filter(lambda x: len(x)>48, data)

        mean = map(np.mean,data) 
        std = map(np.std,data)

        # mean = np.random.choice(mean, (5000))
        # std = np.random.choice(std, (5000))

        plt.figure()
        #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        plt.plot(mean,std, "o") 
        plt.title("Distribution of the sample mean (%s)" % label)
        plt.grid(True)
        plt.xscale("log")
        plt.yscale("log")
        plt.xlabel("Mean")
        plt.ylabel("Std. dev.")
        xlim = plt.xlim()
        ylim = plt.ylim()
        minlim = np.max([np.min(xlim),np.min(ylim)])
        maxlim = np.min([np.max(xlim),np.max(ylim)])
        plt.plot([minlim, maxlim], [minlim, maxlim], "--k", lw=2)
        plt.savefig("CLTMeanVsStd_%s.eps" % label)


def cLTMedianVsMad(results):
    
    # flatten the lists
    rng=None
    for i, label in [ (1, "Inferred"), (0, "Measured")]:
        if resutls[i] is None:
            continue

        data = results[i].values()

        #filtering
        data = filter(lambda x: len(x)>48, data)

        mean = map(np.median,data) 
        std = map(tools.mad,data)

        # mean = np.random.choice(mean, (5000))
        # std = np.random.choice(std, (5000))

        plt.figure()
        #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        plt.plot(mean,std, "o") 
        plt.title("Distribution of the sample mean (%s)" % label)
        plt.grid(True)
        plt.xscale("log")
        plt.yscale("log")
        plt.xlim([10**-4, 10**3])
        plt.ylim([10**-3, 10**3])
        plt.xlabel("Median")
        plt.ylabel("MAD")
        xlim = plt.xlim()
        ylim = plt.ylim()
        minlim = np.max([np.min(xlim),np.min(ylim)])
        maxlim = np.min([np.max(xlim),np.max(ylim)])
        plt.plot([minlim, maxlim], [minlim, maxlim], "--k")
        plt.savefig("CLTMedianVsMad_%s.eps" % label)

# def distributionCLTStd(results):
    
    # # flatten the lists
    # measured = map(np.std,results[0].values()) 
    # inferred = map(np.std,results[1].values()) 
    # rng=None
    # for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        # plt.figure()
        # #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        # plt.hist(data, bins=50,  label=label, range=rng) 
        # plt.legend()
        # plt.grid(True)
        # plt.xscale("log")
        # plt.yscale("log")
        # plt.savefig("distriutionCLTStd_%s.eps" % label)

# def distributionCLTMean(results):
    
    # # flatten the lists
    # measured = map(np.mean,results[0].values()) 
    # inferred = map(np.mean,results[1].values()) 
    # rng=None
    # for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        # plt.figure()
        # #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        # plt.hist(data, bins=50,  label=label, range=rng) 
        # plt.legend()
        # plt.grid(True)
        # plt.xscale("log")
        # plt.yscale("log")
        # plt.savefig("distriutionCLTMean_%s.eps" % label)


def distributionNbSamples(results):
    
    # flatten the lists
    measured = list(itertools.chain.from_iterable(results[2].values()))
    inferred = list(itertools.chain.from_iterable(results[3].values()))
    rng = [0, 8000] 
    for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        plt.figure()
        plt.hist(data, bins=50,  label=label, range=rng)
        plt.legend()
        plt.grid(True)
        #plt.xscale("log")
        plt.yscale("log")
        plt.title("Mean = %s" % np.mean(data))
        plt.savefig("distriutionNbSamples_%s.eps" % label)

def distributionAvgNbSamples(results):
    
    measured = filter(lambda x: len(x)>96, results[2].values())
    inferred  = filter(lambda x: len(x)>96, results[3].values())
    # flatten the lists
    measured = map(np.mean,measured)
    inferred = map(np.mean,inferred)
    rng = None #[3, 204] 
    for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        plt.figure()
        #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        plt.hist(data, bins=500,  label=label, range=rng)
        plt.legend()
        plt.grid(True)
        plt.xscale("log")
        plt.yscale("log")
        plt.title("Mean = %s" % np.mean(data))
        plt.savefig("distriutionAvgNbSamples_%s.eps" % label)

def distributionShapiro(results):

    for data, label, samples in [(results[0], "Measured", results[2]), (results[1], "Inferred", results[3])]:
        plt.figure()
        data0 = []
        for k,v in data.iteritems():
            if len(v)>96:
                data0.append(stats.shapiro(v)[1])


        plt.hist(data0, bins=100, label=label)
        plt.legend()
        plt.grid(True)
        plt.yscale("log")
        plt.xscale("log")
        plt.xlabel("Shapiro p-value")
        plt.title("Mean = %0.3f" % np.mean(data0))
        plt.savefig("distriutionShapiro_%s.eps" % label)
