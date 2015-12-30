import sys
import itertools
from datetime import datetime
from datetime import timedelta
from pytz import timezone
import calendar
import time
import os
import glob
import numpy as np
from collections import defaultdict
from collections import deque
from scipy import stats
import pymongo
from multiprocessing import Process, JoinableQueue, Manager, Pool
import tools
import statsmodels.api as sm
import cPickle as pickle
import pygeoip
import socket
import functools
import pandas as pd

def readOneTraceroute(trace, measuredRtt, inferredRtt, metric=np.nanmedian):
    """Read a single traceroute instance and compute the corresponding 
    measured and inferred RTTs.
    """

    if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
        return measuredRtt, inferredRtt

    probeId = "probe_%s"  % trace["prb_id"]
    probeIp = trace["from"]
    ip2 = None
    prevRttList = {}

    for hopNb, hop in enumerate(trace["result"]):
        # print "i=%s  and hop=%s" % (hopNb, hop)

        try:
            # TODO: clean that workaround results containing no IP, e.g.:
            # {u'result': [{u'x': u'*'}, {u'x': u'*'}, {u'x': u'*'}], u'hop': 6}, 

            if "result" in hop :

                # rttList = np.array([np.nan]*len(hop["result"])) 
                rttList = defaultdict(list) 
                for res in hop["result"]:
                    if not "from" in res  or tools.isPrivateIP(res["from"]) or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    # if hopNb+1!=hop["hop"]:
                        # print trace
                    assert hopNb+1==hop["hop"] or hop["hop"]==255 
                    ip0 = res["from"]
                    rtt =  res["rtt"]
                    # rttList[resNb] = rtt
                    rttList[ip0].append(rtt)


                for ip2, rtts in rttList.iteritems():
                    rttAgg = np.median(rtts)

                    # measured path
                    if not measuredRtt is None:
                        if (probeId, ip2) in measuredRtt:
                            m = measuredRtt[(probeId, ip2)]
                            m["rtt"].append(rttAgg)
                            m["probe"].add(probeIp)
                        else:
                            measuredRtt[(probeId, ip2)] = {"rtt": [rttAgg], 
                                                        "probe": set([probeIp])}

                    # Inferred rtt
                    if not inferredRtt is None and len(prevRttList):
                        for ip1, prevRtt in prevRttList.iteritems():
                            if ip1 == ip2:
                                continue
                            prevRttAgg = np.median(prevRtt)
                            if (ip2,ip1) in inferredRtt:
                                i = inferredRtt[(ip2,ip1)]
                                i["rtt"].append(rttAgg-prevRttAgg)
                                i["probe"].add(probeIp)
                            elif (ip1, ip2) in inferredRtt:
                                i = inferredRtt[(ip1,ip2)]
                                i["rtt"].append(rttAgg-prevRttAgg)
                                i["probe"].add(probeIp)
                            else:
                                inferredRtt[(ip1,ip2)] = {"rtt": [rttAgg-prevRttAgg],
                                                        "probe": set([probeIp])}

        finally:
            prevRttList = rttList
            # TODO we miss 2 inferred links if a router never replies

    return measuredRtt, inferredRtt



######## used by child processes
db = None

def processInit():
    global db
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    db = client.atlas


def computeRtt( (start, end) ):
    """Read traceroutes from a cursor. Used for multi-processing.

    Assume start and end are less than 24h apart
    """
    s = datetime.utcfromtimestamp(start)
    e = datetime.utcfromtimestamp(end)
    collectionNames = set(["traceroute_%s_%02d_%02d" % (d.year, d.month, d.day) for d in [s,e]])

    nbRow = 0
    measuredRtt = None
    inferredRtt = defaultdict(dict)
    for col in collectionNames:
        collection = db[col]
        cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
                projection={"timestamp": 1, "result":1, "prb_id":1, "from":1} , 
                cursor_type=pymongo.cursor.CursorType.EXHAUST,
                batch_size=int(10e6))
        for trace in cursor: 
            readOneTraceroute(trace, measuredRtt, inferredRtt)
            nbRow += 1

    return measuredRtt, inferredRtt, nbRow

######## used by child processes

def mergeRttResults(rttResults, currDate, tsS, nbBins):

        measuredRtt = None
        inferredRtt = defaultdict(dict)
        nbRow = 0 
        for i, (mRtt, iRtt, compRows) in enumerate(rttResults):
            if compRows==0:
                continue

            if not mRtt is None:
                for k, v in mRtt.iteritems():
                    if k in measuredRtt:
                        mea = measuredRtt[k]
                        mea["rtt"].extend(v["rtt"])
                        mea["probe"].update(v["probe"])
                    else:
                        measuredRtt[k] = v

            if not iRtt is None:
                for k, v in iRtt.iteritems():
                    if k in inferredRtt:
                        inf = inferredRtt[k]
                        inf["rtt"].extend(v["rtt"])
                        inf["probe"].update(v["probe"])
                    else:
                        inferredRtt[k] = v

            nbRow += compRows
            timeSpent = (time.time()-tsS)
            sys.stderr.write("\r%s     [%s%s]     %.1f sec,      %.1f row/sec           " % (datetime.utcfromtimestamp(currDate),
                "#"*(30*i/(nbBins-1)), "-"*(30*(nbBins-i)/(nbBins-1)), timeSpent, float(nbRow)/timeSpent))

        return measuredRtt, inferredRtt, nbRow


def outlierDetection(sampleDistributions, smoothMean, param, expId, ts, ip2asn, gi,
    collection=None):

    if sampleDistributions is None:
        return

    alarms = []
    otherParams={}
    metrics = param["metrics"]
    alpha = float(param["alpha"])
    minProbes= param["minASN"]
    minASNEntropy= param["minASNEntropy"]
    confInterval = param["confInterval"]

    for ipPair, data in sampleDistributions.iteritems():

        dist = data["rtt"]
        nbProbes = len(data["probe"])
        asn = defaultdict(int)
        for ip in data["probe"]:
            try:
                if not ip in ip2asn:
                    ip2asn[ip] = str(unicode(gi.asn_by_addr(ip)).partition(" ")[0])
            except socket.error:
                print "Unable to find the ASN for this address: "+ip
                ip2asn[ip] = "Unk"
            asn[ip2asn[ip]] += 1
                
        asnEntropy = stats.entropy(asn.values())/np.log(len(asn))
        n = len(dist) 
        # Compute the distribution median
        if len(asn) < minProbes or asnEntropy < minASNEntropy:
            continue
        med = np.median(dist)
        wilsonCi = sm.stats.proportion_confint(len(dist)/2, len(dist), confInterval, "wilson")
        wilsonCi = np.array(wilsonCi)*len(dist)
        dist.sort()
        currLow = dist[int(wilsonCi[0])]
        currHi = dist[int(wilsonCi[1])]

        if ipPair in smoothMean: 
            # detection
            ref = smoothMean[ipPair]
    
            if ref["high"] < currLow or ref["low"] > currHi:
                if med < ref["mean"]:
                    diff = currHi - ref["low"]
                    diffMed = med - ref["mean"]
                    deviation = diff / (ref["low"]-ref["mean"])
                else:
                    diff = currLow - ref["high"]
                    diffMed = med - ref["mean"]
                    deviation = diff / (ref["high"]-ref["mean"])

                alarm = {"timeBin": ts, "ipPair": ipPair, "currLow": currLow,"currHigh": currHi,
                        "refHigh": ref["high"], "ref":ref["mean"], "refLow":ref["low"], 
                        "median": med, "nbSamples": n, "nbProbes": nbProbes, "deviation": deviation,
                        "diff": diff, "expId": expId, "diffMed": diffMed, "probeASN": list(asn),
                        "nbProbeASN": len(asn), "asnEntropy": asnEntropy}

                if not collection is None:
                    alarms.append(alarm)
            
        # update past data
        if ipPair not in smoothMean: 
            smoothMean[ipPair] = {"mean": float(med), "high": float(currHi), 
                    "low": float(currLow), "probe": set(data["probe"])}  
        else:
            smoothMean[ipPair]["mean"] = (1-alpha)*smoothMean[ipPair]["mean"]+alpha*med
            smoothMean[ipPair]["high"] = (1-alpha)*smoothMean[ipPair]["high"]+alpha*currHi
            smoothMean[ipPair]["low"] = (1-alpha)*smoothMean[ipPair]["low"]+alpha*currLow
            smoothMean[ipPair]["probe"].update(data["probe"]) 


    # Insert all alarms to the database
    if len(alarms) and not collection is None:
        collection.insert_many(alarms)


def detectRttChangesMongo(configFile="detection.cfg"):

    nbProcesses = 6
    binMult = 5
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    # TODO clean this:
    metrics = [np.median, np.median, tools.mad] 

    expParam = {
            "timeWindow": 60*60, # in seconds 
            # "historySize": 24*7,  # 7 days
            "start": datetime(2015, 5, 31, 23, 45, tzinfo=timezone("UTC")), 
            "end":   datetime(2015, 12, 22, 0, 0, tzinfo=timezone("UTC")),
            "alpha": 0.01, 
            "confInterval": 0.05,
            "metrics": str(metrics),
            "minASN": 0,
            "minASNEntropy": 0.,
            "experimentDate": datetime.now(),
            "comment": "all data, no diversity check",
            }

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.rttExperiments
    alarmsCollection = db.rttChanges
    expId = detectionExperiments.insert_one(expParam).inserted_id 

    sampleMedianMeasured = None 
    sampleMedianInferred = {}
    ip2asn = {}
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))
    expParam["metrics"] = metrics

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Rtt analysis %s" % datetime.utcfromtimestamp(currDate))
        tsS = time.time()

        # Get distributions for the current time bin
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (binEdges[i], binEdges[i+1]) )

        measuredRtt = None
        inferredRtt = defaultdict(dict)
        nbRow = 0 
        rttResults =  pool.imap_unordered(computeRtt, params)
        measuredRtt, inferredRtt, nbRow = mergeRttResults(rttResults, currDate, tsS, nbProcesses*binMult)

        # Detect oulier values
        for dist, smoothMean in [(measuredRtt, sampleMedianMeasured),
                (inferredRtt, sampleMedianInferred)]:
            outlierDetection(dist, smoothMean, expParam, expId, 
                    datetime.utcfromtimestamp(currDate), ip2asn, gi, alarmsCollection)

        timeSpent = (time.time()-tsS)
        sys.stderr.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()

    for ref, label in [(sampleMedianMeasured, "measured"), (sampleMedianInferred, "inferred")]:
        if not ref is None:
            print "Writing %s reference to file system." % (label)
            fi = open("saved_references/%s_%s.pickle" % (expId, label), "w")
            pickle.dump(ref, fi, 2) 


def asn_by_addr(ip, db=None):
    try:
        return str(unicode(db.asn_by_addr(ip)).partition(" ")[0])
    except socket.error:
        return "Unk"


import matplotlib.pylab as plt
def eventCharacterization():

    print "Retrieving Alarms"
    db = tools.connect_mongo()
    collection = db.rttChanges

    exp = db.rttExperiments.find_one({}, sort=[("$natural", -1)] )

    cursor = collection.aggregate([
        {"$match": {
            "expId": exp["_id"], # DNS Root 60min time bin
            # "nbProbes": {"$gt": 4},
            }}, 
        {"$project": {
            "ipPair":1,
            "timeBin":1,
            "nbSamples":1,
            "nbProbes":1,
            "diff":1,
            "deviation": 1,
            }},
        {"$unwind": "$ipPair"},
        ])

    df =  pd.DataFrame(list(cursor))
    df["timeBin"] = pd.to_datetime(df["timeBin"],utc=True)
    df.set_index("timeBin")

    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
    fct = functools.partial(asn_by_addr, db=gi)

    # find ASN for each ip
    df["asn"] = df["ipPair"].apply(fct)
    df["absDiff"] = df["diff"].apply(np.abs)

    group = df.groupby("timeBin").sum()
    group["metric"] = group["deviation"]
    events = group[group["metric"]> group["metric"].median()+3*group["metric"].mad()]

    plt.figure()
    plt.plot(group.index, group["metric"])
    plt.savefig("tfidf_metric.eps")
    
    print "Found %s events" % len(events)
    print events

    nbDoc = len(df["timeBin"].unique())

    for bin in events.index:
        maxVal = 0
        maxLabel = ""
        print "Event: %s " % bin
        docLen = len(df[df["timeBin"] == bin])
        plt.figure()
        x = []

        for asn in df["asn"].unique():

            tmp = df[df["asn"] == asn]
            nbDocAsn = float(len(tmp["timeBin"].unique()))
            asnFreq = float(np.sum(tmp["timeBin"] == bin))

            if nbDocAsn > maxVal:
                maxVal = nbDocAsn
                maxLabel = asn

            tfidf = (asnFreq/docLen) * np.log(nbDoc/nbDocAsn)
            if tfidf > 0.007:
                print "\t%s, tfidf=%s" % (asn, tfidf)
                maxVal = tfidf
                maxLabel = asn
            x.append(tfidf)

        print "max asn: %s, %s occurences" % (maxLabel, maxVal)
        plt.hist(x)
        plt.savefig("tfidf_hist_%s.eps" % bin)

    return df

if __name__ == "__main__":
    # testDateRangeMongo(None,save_to_file=True)
    detectRttChangesMongo()

