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
import plot
import random
import re

# import cProfile

from bson import objectid

#TODO try to speed up this function with numba?
def readOneTraceroute(trace, diffRtt, metric=np.nanmedian):
    """Read a single traceroute instance and compute the corresponding 
    differential RTTs.
    """

    if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
        return diffRtt

    # probeId = "probe_%s"  % trace["prb_id"]
    probeIp = trace["from"]
    prevRttMed = {}

    for hopNb, hop in enumerate(trace["result"]):
        # print "i=%s  and hop=%s" % (hopNb, hop)

        try:
            # TODO: clean that workaround results containing no IP, e.g.:
            # {u'result': [{u'x': u'*'}, {u'x': u'*'}, {u'x': u'*'}], u'hop': 6}, 

            if "result" in hop :

                rttList = defaultdict(list) 
                rttMed = {}

                for res in hop["result"]:
                    if not "from" in res  or tools.isPrivateIP(res["from"]) or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    # assert hopNb+1==hop["hop"] or hop["hop"]==255 

                    rttList[res["from"]].append(res["rtt"])

                for ip2, rtts in rttList.iteritems():
                    rttAgg = np.median(rtts)
                    rttMed[ip2] = rttAgg

                    # Differential rtt
                    if len(prevRttMed):
                        for ip1, pRttAgg in prevRttMed.iteritems():
                            if ip1 == ip2 :
                                continue

                            # data for (ip1, ip2) and (ip2, ip1) are put
                            # together in mergeRttResults
                            if (ip1, ip2) in diffRtt:
                                i = diffRtt[(ip1,ip2)]
                                i["rtt"].append(rttAgg-pRttAgg)
                                i["probe"].append(probeIp)
                            else:
                                diffRtt[(ip1,ip2)] = {"rtt": [rttAgg-pRttAgg],
                                                        "probe": [probeIp]}

        finally:
            prevRttMed = rttMed
            # TODO we miss 2 inferred links if a router never replies

    return diffRtt



######## used by child processes
db = None

def processInit():
    global db
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    # client = pymongo.MongoClient("10.206.116.235",connect=True)
    db = client.atlas

# def computeRtt( (start, end) ):
    # results = [None]
    # cProfile.runctx("results[0] = computeRtt2( (start, end) )", globals(), locals())
    # # results = computeRtt2( (start, end) )
    # return results[0]

def computeRtt( (af, start, end, skip, limit, prefixes) ):
    """Read traceroutes from a cursor. Used for multi-processing.

    Assume start and end are less than 24h apart
    """
    s = datetime.utcfromtimestamp(start)
    e = datetime.utcfromtimestamp(end)
    collectionNames = set(["traceroute%s_%s_%02d_%02d" % (af, d.year, d.month, d.day) for d in [s,e]])

    nbRow = 0
    diffRtt = defaultdict(dict)
    for col in collectionNames:
        collection = db[col]
        if prefixes is None:
            cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end} } , 
                projection={"result":1, "from":1} , 
                skip = skip,
                limit = limit,
                # cursor_type=pymongo.cursor.CursorType.EXHAUST,
                batch_size=int(10e6))
        else:
            cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}, "result.result.from": prefixes } , 
                projection={"result":1, "from":1} , 
                skip = skip,
                limit = limit,
                # cursor_type=pymongo.cursor.CursorType.EXHAUST,
                batch_size=int(10e6))
        for trace in cursor: 
            readOneTraceroute(trace, diffRtt)
            nbRow += 1

    return diffRtt, nbRow

######## used by child processes

def mergeRttResults(rttResults, currDate, tsS, nbBins):

        diffRtt = defaultdict(dict)
        nbRow = 0 
        for i, (iRtt, compRows) in enumerate(rttResults):
            if compRows==0:
                continue

            if not iRtt is None:
                for k, v in iRtt.iteritems():

                    # put together data for (ip1, ip2) and (ip2, ip1)
                    ipPair = tuple(sorted(k))
                    if ipPair in diffRtt:
                        inf = diffRtt[ipPair]
                        inf["rtt"].extend(v["rtt"])
                        inf["probe"].extend(v["probe"])
                    else:
                        diffRtt[ipPair] = v

            nbRow += compRows
            timeSpent = (time.time()-tsS)
            if nbBins>1:
                sys.stderr.write("\r%s     [%s%s]     %.1f sec,      %.1f row/sec           " % (datetime.utcfromtimestamp(currDate),
                "#"*(30*i/(nbBins-1)), "-"*(30*(nbBins-i)/(nbBins-1)), timeSpent, float(nbRow)/timeSpent))
            else:
                sys.stderr.write("\r%s     [%s%s]     %.1f sec,      %.1f row/sec           " % (datetime.utcfromtimestamp(currDate),
                "#"*(30*i/(nbBins)), "-"*(30*(nbBins-i)/(nbBins)), timeSpent, float(nbRow)/timeSpent))

        return diffRtt, nbRow


def outlierDetection(sampleDistributions, smoothMean, param, expId, ts, ip2asn, gi,
    collection=None):

    if sampleDistributions is None:
        return

    alarms = []
    otherParams={}
    alpha = float(param["alpha"])
    minAsn= param["minASN"]
    minASNEntropy= param["minASNEntropy"]
    confInterval = param["confInterval"]
    minSeen = param["minSeen"]

    for ipPair, data in sampleDistributions.iteritems():

        dist = np.array(data["rtt"])
        probes = np.array(data["probe"])
        mask = np.array([True]*len(dist))
        asn = defaultdict(int)
        asnProbeIdx = defaultdict(list)

        for idx, ip in enumerate(data["probe"]):
            try:
                if not ip in ip2asn:
                    ip2asn[ip] = str(unicode(gi.asn_by_addr(ip)).partition(" ")[0])
            except socket.error:
                print "Unable to find the ASN for this address: "+ip
                ip2asn[ip] = "Unk"
            a = ip2asn[ip]
            asn[a] += 1
            asnProbeIdx[a].append(idx)
                
        asnEntropy = stats.entropy(asn.values())/np.log(len(asn))
        trimDist = False
       
        # trim the distribution if needed
        while asnEntropy < minASNEntropy and len(asn) > minAsn:

            #remove one sample from the most prominent AS
            maxAsn = max(asn, key=asn.get)
            remove = random.randrange(0,len(asnProbeIdx[maxAsn]))
            rmIdx = asnProbeIdx[maxAsn].pop(remove)
            mask[rmIdx] = False
            asn[maxAsn] -= 1
            #remove the AS if we removed all its probes
            if asn[maxAsn] <= 0:
                asn.pop(maxAsn, None)
        
            # recompute the entropy
            asnEntropy = stats.entropy(asn.values())/np.log(len(asn))
            trimDist = True 

        # Compute the distribution median
        if len(asn) < minAsn or asnEntropy < minASNEntropy:
            continue

        # if trimmed then update the sample dist and probes
        if trimDist:
            dist = dist[mask]
            probes = probes[mask]

        nbProbes = len(probes)
        n = len(dist) 
        med = np.median(dist)
        wilsonCi = sm.stats.proportion_confint(len(dist)/2, len(dist), confInterval, "wilson")
        wilsonCi = np.array(wilsonCi)*len(dist)
        dist.sort()
        currLow = dist[int(wilsonCi[0])]
        currHi = dist[int(wilsonCi[1])]
        reported = False

        if ipPair in smoothMean: 
            # detection
            ref = smoothMean[ipPair]
    
            if ref["nbSeen"] >= minSeen and (ref["high"] < currLow or ref["low"] > currHi):
                if med < ref["mean"]:
                    diff = currHi - ref["low"]
                    diffMed = med - ref["mean"]
                    deviation = diffMed / (ref["low"]-ref["mean"])
                    devBound = diff / (ref["low"]-ref["mean"])
                else:
                    diff = currLow - ref["high"]
                    diffMed = med - ref["mean"]
                    deviation = diffMed / (ref["high"]-ref["mean"])
                    devBound = diff / (ref["high"]-ref["mean"])

                alarm = {"timeBin": ts, "ipPair": ipPair, "currLow": currLow,"currHigh": currHi,
                        "refHigh": ref["high"], "ref":ref["mean"], "refLow":ref["low"], 
                        "median": med, "nbSamples": n, "nbProbes": nbProbes, "deviation": deviation,
                        "diff": diff, "expId": expId, "diffMed": diffMed, "samplePerASN": list(asn),
                        "nbASN": len(asn), "asnEntropy": asnEntropy, "nbSeen": ref["nbSeen"],
                        "devBound": devBound, "trimDist": trimDist}

                reported = True

                if not collection is None:
                    alarms.append(alarm)
            
            # update reference
            ref["nbSeen"] += 1
            if ref["nbSeen"] < minSeen:               # still in the bootstrap
                ref["mean"].append(med)
                ref["high"].append(currHi)
                ref["low"].append(currLow)
            elif ref["nbSeen"] == minSeen:            # end of the bootstrap
                ref["mean"] = float(np.median(ref["mean"]))
                ref["high"] = float(np.median(ref["high"]))
                ref["low"] = float(np.median(ref["low"]))
            else:
                ref["mean"] = (1-alpha)*ref["mean"]+alpha*med
                ref["high"] = (1-alpha)*ref["high"]+alpha*currHi
                ref["low"] = (1-alpha)*ref["low"]+alpha*currLow
            ref["probe"].update(set(data["probe"])) 
            ref["lastSeen"] = ts
            if reported:
                ref["nbReported"] += 1
            if trimDist:
                ref["nbTrimmed"] += 1
        else:
            # add new ref
            if minSeen > 1:
                smoothMean[ipPair] = {"mean": [med], "high": [currHi], 
                    "low": [currLow], "probe": set(data["probe"]), "firstSeen":ts,
                    "lastSeen":ts, "nbSeen":1, "nbReported": 0, "nbTrimmed": int(trimDist) }  
            else:
                smoothMean[ipPair] = {"mean": float(med), "high": float(currHi), 
                    "low": float(currLow), "probe": set(data["probe"]), "firstSeen":ts,
                    "lastSeen":ts, "nbSeen":1, "nbReported": 0 , "nbTrimmed": int(trimDist)}  


    # Insert all alarms to the database
    if len(alarms) and not collection is None:
        collection.insert_many(alarms)


def detectRttChangesMongo(expId=None, configFile="detection.cfg"):

    nbProcesses = 12 
    binMult = 3 # number of bins = binMult*nbProcesses 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.rttExperiments
    alarmsCollection = db.rttChanges

    if expId is None:
        expParam = {
                "timeWindow": 60*60, # in seconds 
                # "historySize": 24*7,  # 7 days
                "start": datetime(2015, 5, 1, 0, 0, tzinfo=timezone("UTC")), 
                # "end":   datetime(2016, 1, 1, 0, 0, tzinfo=timezone("UTC")),
                "end":   datetime(2015, 8, 1, 0, 0, tzinfo=timezone("UTC")),
                # "end":   datetime(2015, 6, 20, 0, 0, tzinfo=timezone("UTC")),
                "alpha": 0.01, 
                "confInterval": 0.05,
                "minASN": 3,
                "minASNEntropy": 0.5,
                "minSeen": 3,
                "experimentDate": datetime.now(),
                "af": "",
                "comment": "60 min Oct to Dec. 2015",
                "prefixes": None
                }

        expId = detectionExperiments.insert_one(expParam).inserted_id 
        sampleMediandiff = {}

    else:
        expParam = detectionExperiments.find_one({"_id": expId})
        expParam["start"] = expParam["end"]
        expParam["end"] = datetime(2015, 9, 1, 0, 0)
        resUpdate = detectionExperiments.replace_one({"_id": expId}, expParam)
        if resUpdate.modified_count != 1:
            print "Problem happened when updating the experiment dates!"
            print resUpdate
            return

        sys.stderr.write("Loading previous reference...")
        fi = open("saved_references/%s_%s.pickle" % (expId, "diffRTT"), "rb")
        sampleMediandiff = pickle.load(fi) 
        sys.stderr.write("done!\n")

    if not expParam["prefixes"] is None:
        expParam["prefixes"] = re.compile(expParam["prefixes"])

    ip2asn = {}
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Rtt analysis %s" % datetime.utcfromtimestamp(currDate))
        tsS = time.time()

        # Get distributions for the current time bin
        c = datetime.utcfromtimestamp(currDate)
        col = "traceroute%s_%s_%02d_%02d" % (expParam["af"], c.year, c.month, c.day) 
        if expParam["prefixes"] is None:
            totalRows = db[col].count({ "timestamp": {"$gte": currDate, "$lt": currDate+expParam["timeWindow"]}})
        else:
            totalRows = db[col].count({ "timestamp": {"$gte": currDate, "$lt": currDate+expParam["timeWindow"]}, "result.result.from": expParam["prefixes"] })
        if not totalRows:
            print "No data for that time bin!"
            continue
        params = []
        limit = int(totalRows/(nbProcesses*binMult-1))
        skip = range(0, totalRows, limit)
        for i, val in enumerate(skip):
            params.append( (expParam["af"], currDate, currDate+expParam["timeWindow"], val, limit, expParam["prefixes"]) )

        diffRtt = defaultdict(dict)
        nbRow = 0 
        rttResults =  pool.imap_unordered(computeRtt, params)
        diffRtt, nbRow = mergeRttResults(rttResults, currDate, tsS, nbProcesses*binMult)

        # Detect oulier values
        for dist, smoothMean in [ (diffRtt, sampleMediandiff)]:
            outlierDetection(dist, smoothMean, expParam, expId, 
                    datetime.utcfromtimestamp(currDate), ip2asn, gi, alarmsCollection)

        timeSpent = (time.time()-tsS)
        sys.stderr.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    pool.close()
    pool.join()

    for ref, label in [(sampleMediandiff, "diffRTT")]:
        if not ref is None:
            print "Writing %s reference to file system." % (label)
            fi = open("saved_references/%s_%s.pickle" % (expId, label), "w")
            pickle.dump(ref, fi, 2) 


if __name__ == "__main__":
    # testDateRangeMongo(None,save_to_file=True)
    expId = None
    if len(sys.argv)>1:
        expId = objectid.ObjectId(sys.argv[1]) 
    detectRttChangesMongo(expId)

