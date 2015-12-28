import statsmodels.api as sm
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
import calendar
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
    collection = db.traceroute_2015_12

def computeRtt( (start, end) ):
    """Read traceroutes from a cursor. Used for multi-processing.
    """

    tsS = time.time()
    nbRow = 0
    measuredRtt = defaultdict(list)
    inferredRtt = defaultdict(list)
    tsM = time.time()
    cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end},
            # "result": { "$elemMatch": { "result": {"$elemMatch": {"from": ip2}} } }
            } , 
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
def manyRttEvolution(res, minPts=2000):
    
    nbAlarms = 0
    for k, v in res[0].iteritems():
        if len(v)>minPts:
            nbAlarms += rttEvolution( (v, res[1][k]), k)

    print " %s alarms in total" % nbAlarms

def rttEvolution(res, ips):
    res = np.array(res)
    smoothAvg = []
    smoothHi = []
    smoothLow = []
    alarms = []
    median = []
    mean = []
    ciLow = []
    ciHigh = []
    dates = []
    start = np.min(res[1])
    end = np.max(res[1])
    diff = end-start
    dateRange = [start+timedelta(hours=x) for x in range(diff.days*24)]

    for d in dateRange:
        indices = res[1]==d
        dist = res[0][indices]
        if np.sum(indices) == 0 or np.sum(dist) < 18:
            continue
        dates.append(d)
        median.append(np.median(dist))
        mean.append(np.mean(dist))
        dist.sort()
        wilsonCi = sm.stats.proportion_confint(len(dist)/2, len(dist), 0.05, "wilson")
        wilsonCi = np.array(wilsonCi)*len(dist)
        ciLow.append( median[-1] - dist[int(wilsonCi[0])] )
        ciHigh.append( dist[int(wilsonCi[1])] - median[-1] )

        if not len(smoothAvg):
            smoothAvg.append(median[-1])
            smoothHi.append(dist[int(wilsonCi[1])])
            smoothLow.append(dist[int(wilsonCi[0])])
        else:
            smoothAvg.append(0.99*smoothAvg[-1]+0.01*median[-1])
            smoothHi.append(0.99*smoothHi[-1]+0.01*dist[int(wilsonCi[1])])
            smoothLow.append(0.99*smoothLow[-1]+0.01*dist[int(wilsonCi[0])])


            if median[-1]-ciLow[-1] > smoothHi[-1] or median[-1]+ciHigh[-1] < smoothLow[-1]: 
                alarms.append(d)

        # if np.min(dist) > 9.0:
            # print dist
            # print median[-1]
            # print  median[-1] - dist[int(wilsonCi[0])] 
            # print  dist[int(wilsonCi[1])] - median[-1] 

    fig = plt.figure(figsize=(10,4))
    plt.errorbar(dates, median, [ciLow, ciHigh], ecolor='g')
    plt.plot(dates, smoothAvg, 'r-')
    plt.plot(dates, smoothHi, 'k--')
    plt.plot(alarms, [10]*len(alarms), "r*")
    plt.grid(True)
    plt.title("%s - %s" % ips)
    fig.autofmt_xdate()
    plt.savefig("fig/rawData/%s_%s_%sAlarms_rttModel.eps" % (ips[0], ips[1], len(alarms)))
    plt.close()

    fig = plt.figure()
    plt.plot(res[1], res[0],"x")
    plt.grid(True)
    # plt.yscale("log")
    plt.title("%s - %s" % ips)
    fig.autofmt_xdate() 
    plt.savefig("fig/rawData/%s_%s_rttRawData.eps" % ips)
    plt.close()

    return len(alarms)

def getRttData():
    """

Notes: takes about 6G of RAM for 1 week of data for 1 measurement id
    """

    nbProcesses = 6
    binMult = 5 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    expParam = {
            "timeWindow": 60*60, # in seconds 
            "start": datetime(2015, 11, 24, 23, 45, tzinfo=timezone("UTC")), 
            "end":   datetime(2015, 12, 3, 0, 0, tzinfo=timezone("UTC")),
            "experimentDate": datetime.now(),
            }

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))

    rawRttMeasured = defaultdict(list)
    rawRttMeasuredDate = defaultdict(list) 
    rawRttInferred = defaultdict(list)
    rawRttInferredDate = defaultdict(list) 

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Rtt analysis %s" % datetime.utcfromtimestamp(currDate))
        tsS = time.time()
        currDatetime = datetime.utcfromtimestamp(currDate)

        # Get distributions for the current time bin
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (binEdges[i], binEdges[i+1]) )

        measuredRtt = defaultdict(list)
        inferredRtt = defaultdict(list)
        nbRow = 0 
        rttResults =  pool.imap_unordered(computeRtt, params)
        measuredRtt, inferredRtt, nbRow = rttAnalysis.mergeRttResults(rttResults)

        for k,v in measuredRtt.iteritems():
            rawRttMeasured[k].extend(v)
            rawRttMeasuredDate[k].extend([currDatetime]*len(v))
        for k,v in inferredRtt.iteritems():
            rawRttInferred[k].extend(v)
            rawRttInferredDate[k].extend([currDatetime]*len(v))

        timeSpent = (time.time()-tsS)
        sys.stderr.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    
    return (rawRttMeasured, rawRttMeasuredDate, rawRttInferred, rawRttInferredDate  )


def nbRttChanges():

    db = tools.connect_mongo()
    collection = db.rttChanges
    fig = plt.figure(figsize=(10,4))

    exp = db.rttExperiments.find_one({}, sort=[("$natural", -1)] )

    for label, filt in [
            # ("Measured", {"$regex": re.compile("probe.*")}),
            ("Inferred", {"$not": re.compile("probe.*")})
            ]:
        # for expId in expIds:
        cursor = collection.aggregate([
            {"$project": {
                "expId":1,
                "deviation":1,
                "diff":1,
                "ipPair":1,
                "timeBin":1,
                "nbSamples":1,
                "nbProbes":1,
                "abs": {
                        "$cond": [
                                  { "$lt": ['$diff', 0] },
                                        { "$subtract": [0, '$diff'] }, 
                                              '$diff'
                                                  ]
                        }
                # "mag": {"$multiply": ["$nbSamples", "$diff"]}
                }},
            {"$match": {
                "expId": exp["_id"], # DNS Root 60min time bin
                # "expId": objectid.ObjectId("5675143af789374697cbb0d5"), # DNS Root 60min time bin
                # "expId": objectid.ObjectId("56711932f78937703ed7df46"), # 60min time bin
                # "expId": objectid.ObjectId("56711970f78937706117560d"), # 30min time bin
                # "expId": objectid.ObjectId("566e8dd2f7893720f9089ac9"), # Wrong
                # median for the inferred rtt
                # "deviation": {"$gt":0}, 
                # "diff": {"$gt":0}, 
                "ipPair.0": filt,
                # "nbProbes": {"$gt": 4},
                # "ipPair.0": {"$not": re.compile("probe.*")}
                }}, 
                # "ipPair.0": {"$regex": re.compile("probe.*")}}}, 
            {"$group":{"_id": "$timeBin", "count": {"$sum": "$abs"}}},
            # {"$group":{"_id": "$timeBin", "count": {"$sum":"$deviation"}}},
            {"$sort": {"_id": 1}}
            ])

        df =  pd.DataFrame(list(cursor))
        df["_id"] = pd.to_datetime(df["_id"],utc=True)
        df.set_index("_id")

        # plt.plot_date(df["_id"],df["count"],tz=timezone("UTC"))
        plt.plot(df["_id"],df["count"], label=label)

        fig.autofmt_xdate()
        plt.legend()
        plt.grid(True)
        plt.ylabel("Accumulated deviation difference")
        plt.yscale("log")
        plt.show()
        
    plt.savefig("fig/rttChanges_wilson.eps")
    
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
            # {"$match": {"dst_ip": dst_ip, "expId": objectid.ObjectId("566e8c41f789371f9038e813") }}, #, "corr": {"$lt": 0.05}}},
            # {"$match": {"dst_ip": dst_ip, "expId": objectid.ObjectId("5675143df7893746b3729fd0") }}, #, "corr": {"$lt": 0.05}}},
            {"$match": {"dst_ip": dst_ip, "expId": objectid.ObjectId("56775b72f789370348d58a02"),
            "corr": {"$lt": 0.05}}},
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
        if not len(df):
            continue
        df["_id"] = pd.to_datetime(df["_id"],utc=True)
        df.set_index("_id")

        print "########## %s #############" % dst_ip
        event = df.ix[df["count"]> np.mean(df["count"])+3*np.std(df["count"])]["_id"]
        print event
    
        fig = plt.figure()
        # plt.plot_date(df["_id"],df["count"],tz=timezone("UTC"))
        plt.plot(df["_id"],df["count"]/3,label=dst_ip)
        # plt.plot(event, [0]*len(event), "r*")

        fig.autofmt_xdate()
        plt.grid(True)
        plt.ylabel("Nb. alarms")
        plt.yscale("log")
        plt.legend()
        plt.savefig("fig/routeChange_%s.eps" % dst_ip)
    

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
        plt.savefig("fig/nbSampleVsShapiro_%s.eps" % label)



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
        plt.savefig("fig/CLTMeanVsStd_%s.eps" % label)


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
        plt.savefig("fig/CLTMedianVsMad_%s.eps" % label)

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
        # plt.savefig("fig/distriutionCLTStd_%s.eps" % label)

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
        # plt.savefig("fig/distriutionCLTMean_%s.eps" % label)


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
        plt.savefig("fig/distriutionNbSamples_%s.eps" % label)

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
        plt.savefig("fig/distriutionAvgNbSamples_%s.eps" % label)

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
        plt.savefig("fig/distriutionShapiro_%s.eps" % label)
