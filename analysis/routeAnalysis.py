import sys
import itertools
from datetime import datetime
from datetime import timedelta
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

def readOneTraceroute(trace, measuredRtt, inferredRtt, metric=np.nanmedian):
    """Read a single traceroute instance and compute the corresponding 
    measured and inferred RTTs.
    """

    if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
        return measuredRtt, inferredRtt

    ipProbe = "probe_%s"  % trace["prb_id"]
    ip1 = None
    ip2 = None

    for hopNb, hop in enumerate(trace["result"]):
        # print "i=%s  and hop=%s" % (hopNb, hop)

        try:
            # TODO: clean that workaround results containing no IP, e.g.:
            # {u'result': [{u'x': u'*'}, {u'x': u'*'}, {u'x': u'*'}], u'hop': 6}, 

            if "result" in hop :

                rttList = np.array([np.nan]*len(hop["result"])) 
                for resNb, res in enumerate(hop["result"]):
                    if not "from" in res  or tools.isPrivateIP(res["from"]) or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    # if hopNb+1!=hop["hop"]:
                        # print trace
                    assert hopNb+1==hop["hop"] or hop["hop"]==255 
                    ip2 = res["from"]
                    rtt =  res["rtt"]
                    rttList[resNb] = rtt

                    # measured path
                    if not measuredRtt is None:
                        measuredRtt[(ipProbe, ip2)].append(rtt)

                    # Inferred rtt
                    if not inferredRtt is None and not ip1 is None and not np.all(np.isnan(prevRttList)) and ip1!=ip2:
                        if (ip2,ip1) in inferredRtt:
                            inferredRtt[(ip2,ip1)].append(rtt-prevRttAgg)
                        else:
                            inferredRtt[(ip1,ip2)].append(rtt-prevRttAgg)
        finally:
            prevRttList = rttList
            # TODO we miss 2 inferred links if a router never replies
            if not np.all(np.isnan(prevRttList)):
                prevRttAgg = metric(prevRttList)
            ip1 = ip2

    return measuredRtt, inferredRtt



######## used by child processes
collection = None

def processInit():
    global collection
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    db = client.atlas
    collection = db.traceroute

def computeRtt( (start, end) ):
    """Read traceroutes from a cursor. Used for multi-processing.
    """

    tsS = time.time()
    nbRow = 0
    measuredRtt = defaultdict(list)
    inferredRtt = defaultdict(list)
    tsM = time.time()
    cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
            projection={"timestamp": 1, "result":1, "prb_id":1} , 
            cursor_type=pymongo.cursor.CursorType.EXHAUST)
    tsM = time.time() - tsM
    for trace in cursor: 
        readOneTraceroute(trace, measuredRtt, inferredRtt)
        nbRow += 1
    timeSpent = time.time()-tsS
    # print("Worker %0.1f /sec., dict size: (%s, %s), mongo time: %s, total time: %s"
            # % (float(nbRow)/(timeSpent), len(measuredRtt),len(inferredRtt), tsM, timeSpent))

    return measuredRtt, inferredRtt, nbRow

######## used by child processes

def mergeRttResults(rttResults):

        measuredRtt = defaultdict(list)
        inferredRtt = defaultdict(list)
        nbRow = 0 
        for mRtt, iRtt, compRows in rttResults:
            for k, v in mRtt.iteritems():
                measuredRtt[k].extend(v)

            for k, v in iRtt.iteritems():
                inferredRtt[k].extend(v)

            nbRow += compRows

        return measuredRtt, inferredRtt, nbRow


def outlierDetection(sampleDistributions, pastMean, pastMeanDiff, param, expId, ts, 

    collection=None, updatePastData=True):

    alarms = []
    otherParams={}
    metrics = param["metrics"]
    tau = param["tau"]
    historySize = param["historySize"]
    minSample = param["minSample"]

    for ipPair, dist in sampleDistributions.iteritems():

        n = len(dist) 
        # Compute the distribution mean
        if n < minSample:
            continue
        sn = metrics[0](dist, dtype=np.float64)

        pMeanDiff = pastMeanDiff[ipPair]
        if len(pMeanDiff) == historySize:       # if the bootstrap is done
            pMean = pastMean[ipPair]
            mu = metrics[1](pMean["mean"])   

            # Estimate parameters for distribution of sqrt(n)*(sn-mu)
            ref = metrics[1](pMeanDiff)   
            refVar    = metrics[2](pMeanDiff)
            boundary = ref + (refVar*tau)

            testPoint = np.sqrt(n)*(sn-mu)
            if testPoint > boundary:
                confidence = 0
                if refVar != 0:
                    confidence = (testPoint - ref)/refVar

                alarm = {"timeBin": ts, "ipPair": ipPair, "score": testPoint,
                        "refBoundary": boundary, "ref":ref, "refVar":refVar, "mu":mu,
                        "sn": sn, "nbSamples": n, "confidence": confidence,
                        "expId": expId}

                if collection is None:
                    # Write the result to the standard output
                    print alarm 
                else:
                    alarms.append(alarm)
        
        if updatePastData:

            if ipPair not in pastMean: 
                pastMean[ipPair] = {"mean": deque(maxlen=historySize), "nbSamp": deque(maxlen=historySize)}

            pMean = pastMean[ipPair]
            prevLen = len(pMean["mean"])

            pMean["mean"].append(sn)
            pMean["nbSamp"].append(n)

            if prevLen != historySize and len(pMean["mean"]) == historySize:
                # Got all data for bootstrap
                # compute the past: sqrt(n)*(S_n - \mu)
                mu = np.median(pMean["mean"])
                for sn, n in zip(pMean["mean"], pMean["nbSamp"]):
                    
                    pMeanDiff.append(np.sqrt(n)*(sn-mu))

                # End of the bootstrap!

    # Insert all alarms to the database
    if len(alarms) and not collection is None:
        collection.insert_many(alarms)


def detectRouteChangesMongo(configFile="detection.cfg"): # todo config file implementation

    nbProcesses = 6
    binMult = 10
    pool = Pool(nbProcesses,initializer=processInit) 

    expParam = {
            "timeWindow": 30*60, # 30 minutes
            "start": datetime(2015, 5, 31, 23, 45), 
            "end":   datetime(2015, 7, 1, 0, 0),
            "msmIDs": range(5001,5027),
            "tau": 3*1.4826, # multiply by 1.4826 in case of MAD 
            "historySize": (86401/1800)*1,  # 3 days
            "experimentDate": datetime.now(),
            }

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.routeExperiments
    alarmsCollection = db.alarms
    expId = detectionExperiments.insert_one(expParam).inserted_id 

    packetPerEdge = defaultdict(lambda : deque(maxlen=expParam["historySize"]))
    packetPerNode = defaultdict(lambda : deque(maxlen=expParam["historySize"]))

    start = int(time.mktime(expParam["start"].timetuple()))
    end = int(time.mktime(expParam["end"].timetuple()))

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Analyzing %s " % currDate)
        tsS = time.time()

        # count packet routes for the current time bin
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (binEdges[i], binEdges[i+1]) )

        measuredRtt = defaultdict(list)
        inferredRtt = defaultdict(list)
        nbRow = 0 
        routeCounts =  pool.imap_unordered(countRoutes, params) # ----
        measuredRtt, inferredRtt, nbRow = mergeRttResults(rttResults)

        # Detect oulier values
        for dist, pastMean, pastMeanDiff in [(measuredRtt, sampleMeanMeasured, meanDiffMeasured),
                (inferredRtt, sampleMeanInferred, meanDiffInferred)]:
            outlierDetection(dist, pastMean, pastMeanDiff, expParam, expId, 
                    datetime.fromtimestamp(currDate), alarmsCollection)

        timeSpent = (time.time()-tsS)
        sys.stderr.write("Done in %s seconds,  %s row/sec\n" % (timeSpent, float(nbRow)/timeSpent))
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    

if __name__ == "__main__":
    # testDateRangeMongo(None,save_to_file=True)
    detectRouteChangesMongo()
