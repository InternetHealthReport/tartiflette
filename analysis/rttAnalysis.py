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

# import cProfile

from bson import objectid

def readOneTraceroute(trace, measuredRtt, inferredRtt, metric=np.nanmedian):
    """Read a single traceroute instance and compute the corresponding 
    measured and inferred RTTs.
    """

    if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
        return measuredRtt, inferredRtt

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

                    # measured path
                    # if not measuredRtt is None:
                        # if (probeId, ip2) in measuredRtt:
                            # m = measuredRtt[(probeId, ip2)]
                            # m["rtt"].append(rttAgg)
                            # m["probe"].add(probeIp)
                        # else:
                            # measuredRtt[(probeId, ip2)] = {"rtt": [rttAgg], 
                                                        # "probe": set([probeIp])}

                    # Differential rtt
                    if len(prevRttMed):
                        for ip1, pRttAgg in prevRttMed.iteritems():
                            if ip1 == ip2:
                                continue

                            if (ip2,ip1) in inferredRtt:
                                i = inferredRtt[(ip2,ip1)]
                                i["rtt"].append(rttAgg-pRttAgg)
                                i["probe"].add(probeIp)
                            elif (ip1, ip2) in inferredRtt:
                                i = inferredRtt[(ip1,ip2)]
                                i["rtt"].append(rttAgg-pRttAgg)
                                i["probe"].add(probeIp)
                            else:
                                inferredRtt[(ip1,ip2)] = {"rtt": [rttAgg-pRttAgg],
                                                        "probe": set([probeIp])}

        finally:
            prevRttMed = rttMed
            # TODO we miss 2 inferred links if a router never replies

    return measuredRtt, inferredRtt



######## used by child processes
db = None

def processInit():
    global db
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    db = client.atlas

# def computeRtt( (start, end) ):
    # results = [None]
    # cProfile.runctx("results[0] = computeRtt2( (start, end) )", globals(), locals())
    # # results = computeRtt2( (start, end) )
    # return results[0]

def computeRtt( (af, start, end, skip, limit) ):
    """Read traceroutes from a cursor. Used for multi-processing.

    Assume start and end are less than 24h apart
    """
    s = datetime.utcfromtimestamp(start)
    e = datetime.utcfromtimestamp(end)
    collectionNames = set(["traceroute%s_%s_%02d_%02d" % (af, d.year, d.month, d.day) for d in [s,e]])

    nbRow = 0
    measuredRtt = None
    inferredRtt = defaultdict(dict)
    for col in collectionNames:
        collection = db[col]
        cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
                projection={"result":1, "from":1} , 
                skip = skip,
                limit = limit,
                # cursor_type=pymongo.cursor.CursorType.EXHAUST,
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
    alpha = float(param["alpha"])
    minAsn= param["minASN"]
    minASNEntropy= param["minASNEntropy"]
    confInterval = param["confInterval"]
    minSeen = param["minSeen"]

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
        if len(asn) < minAsn or asnEntropy < minASNEntropy:
            continue
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
                        "diff": diff, "expId": expId, "diffMed": diffMed, "probeASN": list(asn),
                        "nbProbeASN": len(asn), "asnEntropy": asnEntropy, "nbSeen": ref["nbSeen"],
                        "devBound": devBound}

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
            ref["probe"].update(data["probe"]) 
            ref["lastSeen"] = ts
            if reported:
                ref["nbReported"] += 1
        else:
            # add new ref
            if minSeen > 1:
                smoothMean[ipPair] = {"mean": [med], "high": [currHi], 
                    "low": [currLow], "probe": set(data["probe"]), "firstSeen":ts,
                    "lastSeen":ts, "nbSeen":1, "nbReported": 0 }  
            else:
                smoothMean[ipPair] = {"mean": float(med), "high": float(currHi), 
                    "low": float(currLow), "probe": set(data["probe"]), "firstSeen":ts,
                    "lastSeen":ts, "nbSeen":1, "nbReported": 0 }  


    # Insert all alarms to the database
    if len(alarms) and not collection is None:
        collection.insert_many(alarms)


def detectRttChangesMongo(configFile="detection.cfg"):

    nbProcesses = 6 
    binMult = 4 # number of bins = binMult*nbProcesses 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    expParam = {
            "timeWindow": 60*60, # in seconds 
            # "historySize": 24*7,  # 7 days
            "start": datetime(2015, 6, 1, 0, 0, tzinfo=timezone("UTC")), 
            "end":   datetime(2016, 1, 1, 0, 0, tzinfo=timezone("UTC")),
            # "end":   datetime(2015, 6, 20, 0, 0, tzinfo=timezone("UTC")),
            "alpha": 0.5, 
            "confInterval": 0.05,
            "minASN": 3,
            "minASNEntropy": 0.5,
            "minSeen": 3,
            "experimentDate": datetime.now(),
            "af": "",
            "comment": "60 min all data",
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

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Rtt analysis %s" % datetime.utcfromtimestamp(currDate))
        tsS = time.time()

        # Get distributions for the current time bin
        c = datetime.utcfromtimestamp(currDate)
        col = "traceroute%s_%s_%02d_%02d" % (expParam["af"], c.year, c.month, c.day) 
        totalRows = db[col].count({ "timestamp": {"$gte": currDate, "$lt": currDate+expParam["timeWindow"]}})
        if not totalRows:
            print "No data for that time bin!"
            continue
        params = []
        limit = int(totalRows/(nbProcesses*binMult-1))
        skip = range(0, totalRows, limit)
        for i, val in enumerate(skip):
            params.append( (expParam["af"], currDate, currDate+expParam["timeWindow"], val, limit) )

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




def refStats(ref=None):

    if ref is None:
        # ref = pickle.load(open("./saved_references/567f808ff7893768932b8334_inferred.pickle"))
        ref = pickle.load(open("./saved_references/5680de2af789371baee2d573_inferred.pickle"))

    print "%s ip pairs" % len(ref)

    confUp = map(lambda x: x["high"] - x["mean"], ref.itervalues())
    confDown = map(lambda x: x["mean"] - x["low"], ref.itervalues())
    confSize = map(lambda x: x["high"] - x["low"], ref.itervalues())

    print "upper confidence interval size: %s (+- %s)" % (np.mean(confUp), np.std(confUp))
    print "\t min=%s max=%s" % (np.min(confUp), np.max(confUp))
    print "lower confidence interval size: %s (+- %s)" % (np.mean(confDown), np.std(confDown))
    print "\t min=%s max=%s" % (np.min(confDown), np.max(confDown))
    print "confidence interval size: %s (+- %s)" % (np.mean(confSize), np.std(confSize))
    print "confidence interval size: %s (+- %s) median" % (np.median(confSize), tools.mad(confSize))
    print "\t min=%s max=%s" % (np.min(confSize), np.max(confSize))

    # plot.plt.figure()
    # # plot.plt.hist(confUp, bins=50, log=True)
    # plot.ecdf(confUp)
    # plot.plt.xscale("log")
    # # plot.plt.yscale("log")
    # plot.plt.grid(True)
    # plot.plt.xlabel("Confidence interval size (high)")
    # plot.plt.savefig("fig/rttChange_ref_confIntervalHigh.eps")

    # plot.plt.figure()
    # # plt.hist(confDown,bins=50, log=True)
    # plot.ecdf(confDown)
    # plot.plt.xscale("log")
    # # plot.plt.yscale("log")
    # plot.plt.grid(True)
    # plot.plt.xlabel("Confidence interval size (low)")
    # plot.plt.savefig("fig/rttChange_ref_confIntervalLow.eps")

    plot.plt.figure()
    # plot.plt.hist(confSize,bins=50, log=True)
    plot.ecdf(confSize, label="interval size")
    plot.ecdf(confDown, label="lower bound")
    plot.ecdf(confUp, label="upper bound")
    plot.plt.xscale("log")
    # plot.plt.yscale("log")
    plot.plt.grid(True)
    plot.plt.legend()
    plot.plt.xlim([10**-4, 10**4])
    plot.plt.xlabel("Confidence interval size")
    plot.plt.savefig("fig/rttChange_ref_confInterval.eps")

    nbProbes = map(lambda x: len(x["probe"]), ref.itervalues())
    print "total number of probes: %s (+- %s)" % (np.mean(nbProbes), np.std(nbProbes))
    print "total number of probes: %s (+- %s) median" % (np.median(nbProbes), tools.mad(nbProbes))
    print "\t min=%s max=%s" % (np.min(nbProbes), np.max(nbProbes))

    plot.plt.figure()
    # plot.plt.hist(nbProbes,bins=50, log=True)
    plot.ecdf(nbProbes)
    plot.plt.xscale("log")
    # plot.plt.yscale("log")
    plot.plt.grid(True)
    plot.plt.xlabel("Number of probes")
    plot.plt.savefig("fig/rttChange_ref_nbProbes.eps")
    plot.plt.close()

    # correlation between the number of probes and the conf. interval size?

    confSize = np.array(confSize)
    idx0 = (confSize < 30000000)
    nbProbes = np.array(nbProbes)
    idx1 = (nbProbes < 65000000000)
    print np.corrcoef(confSize[idx0 & idx1], nbProbes[idx0 & idx1])
    print stats.spearmanr(confSize[idx0 & idx1], nbProbes[idx0 & idx1])


if __name__ == "__main__":
    # testDateRangeMongo(None,save_to_file=True)
    detectRttChangesMongo()

