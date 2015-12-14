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
            cursor_type=pymongo.cursor.CursorType.EXHAUST,
            batch_size=int(10e6))
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


def getMedianSamplesMongo(start = datetime(2015, 6, 7, 23, 45), 
        end = datetime(2015, 6, 13, 23, 59), msmIDs = range(5001,5027),save_to_file=False):

    nbProcesses = 6
    binMult = 5 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    timeWindow = 30*60  # 30 minutes
    medianRttMeasured = defaultdict(list)
    nbSamplesMeasured = defaultdict(list)
    medianRttInferred = defaultdict(list)
    nbSamplesInferred = defaultdict(list)


    start = int(time.mktime(start.timetuple()))
    end = int(time.mktime(end.timetuple()))

    for currDate in range(start,end,timeWindow):
        sys.stderr.write("Analyzing %s " % currDate)
        tsS = time.time()

        params = []
        binEdges = np.linspace(currDate, currDate+timeWindow, nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (binEdges[i], binEdges[i+1]) )

        measuredRtt = defaultdict(list)
        inferredRtt = defaultdict(list)
        nbRow = 0 
        rttResults =  pool.imap_unordered(computeRtt, params)
        measuredRtt, inferredRtt, nbRow = mergeRttResults(rttResults)

        # Computing samples median
        for k, v in measuredRtt.iteritems():
            medianRttMeasured[k].append(np.median(v))
            nbSamplesMeasured[k].append(len(v))
        for k, v in inferredRtt.iteritems():
            medianRttInferred[k].append(np.median(v))
            nbSamplesInferred[k].append(len(v))

        timeSpent = (time.time()-tsS)
        sys.stderr.write("Done in %s seconds,  %s row/sec\n" % (timeSpent, float(nbRow)/timeSpent))
        # readOneTraceroute(trace, rttMeasured, rttInferred)
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    
    result = (medianRttMeasured, medianRttInferred, nbSamplesMeasured, nbSamplesInferred)
    if save_to_file:
        fp = open("test_1day.json","w")
        json.dump(result, fp)

    return result 


def outlierDetection(sampleDistributions, pastMean, pastMeanDiff, param, expId, ts, 
    collection=None):

    alarms = []
    otherParams={}
    metrics = param["metrics"]
    tau = param["tau"]
    historySize = param["historySize"]
    minSamples = param["minSamples"]

    for ipPair, dist in sampleDistributions.iteritems():

        n = len(dist) 
        # Compute the distribution mean
        if n < minSamples:
            continue
        sn = metrics[0](dist, dtype=np.float64)

        # update past data
        if ipPair not in pastMean: 
            pastMean[ipPair] = {"mean": deque(maxlen=historySize), "nbSamp": deque(maxlen=historySize)}
            pastMeanDiff[ipPair] = deque(maxlen=historySize) 

        pMean = pastMean[ipPair]
        pMeanDiff = pastMeanDiff[ipPair]
        if len(pMeanDiff) == historySize:       # if the bootstrap is done
            pMean = pastMean[ipPair]
            mu = metrics[1](pMean["mean"])   

            # Estimate parameters for distribution of sqrt(n)*(sn-mu)
            ref = metrics[1](pMeanDiff)   
            refMean = np.mean(pMeanDiff)   
            refVar    = metrics[2](pMeanDiff)*1.4826  # for MAD it should be multiplied by 1.4826
            refStd    = np.std(pMeanDiff)  # for MAD it should be multiplied by 1.4826
            boundary = ref + (refVar*tau)

            testPoint = np.sqrt(n)*(sn-mu)
            # Update pMeanDiff with new value
            pMeanDiff.append(testPoint)

            if testPoint > boundary:
                confidence = 0
                if refVar != 0:
                    confidence = (testPoint - ref)/refVar
                    confidenceMean = (testPoint - refMean)/refStd

                alarm = {"timeBin": ts, "ipPair": ipPair, "score": testPoint,
                        "refBoundary": boundary, "ref":ref, "refVar":refVar, "mu":mu,
                        "sn": sn, "nbSamples": n, "confidence": confidence,
                        "confidenceMean": confidenceMean, "refMean": refMean, "refStd": refStd,
                        "expId": expId}

                if not collection is None:
                    alarms.append(alarm)
        
        pMean["mean"].append(sn)
        pMean["nbSamp"].append(n)

        if len(pMean["mean"]) >= historySize and len(pMeanDiff) < historySize:
            # Got all data for bootstrap
            # compute the past: sqrt(n)*(S_n - \mu)
            mu = np.median(pMean["mean"])
            for sn, n in zip(pMean["mean"], pMean["nbSamp"]):
                pMeanDiff.append(np.sqrt(n)*(sn-mu))


    # Insert all alarms to the database
    if len(alarms) and not collection is None:
        collection.insert_many(alarms)


def detectRttChangesMongo(configFile="detection.cfg"):

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

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.rttExperiments
    alarmsCollection = db.rttChanges
    expId = detectionExperiments.insert_one(expParam).inserted_id 

    sampleMeanMeasured = {} 
    sampleMeanInferred = {}
    meanDiffMeasured = {}
    meanDiffInferred = {}

    start = int(time.mktime(expParam["start"].timetuple()))
    end = int(time.mktime(expParam["end"].timetuple()))
    expParam["metrics"] = metrics

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Rtt analysis %s" % datetime.fromtimestamp(currDate))
        tsS = time.time()

        # Get distributions for the current time bin
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (binEdges[i], binEdges[i+1]) )

        measuredRtt = defaultdict(list)
        inferredRtt = defaultdict(list)
        nbRow = 0 
        rttResults =  pool.imap_unordered(computeRtt, params)
        measuredRtt, inferredRtt, nbRow = mergeRttResults(rttResults)

        # Detect oulier values
        for dist, pastMean, pastMeanDiff in [(measuredRtt, sampleMeanMeasured, meanDiffMeasured),
                (inferredRtt, sampleMeanInferred, meanDiffInferred)]:
            outlierDetection(dist, pastMean, pastMeanDiff, expParam, expId, 
                    datetime.fromtimestamp(currDate), alarmsCollection)

        timeSpent = (time.time()-tsS)
        sys.stderr.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    

def testDateRangeFS(g,start = datetime(2015, 5, 10, 23, 45), 
        end = datetime(2015, 5, 12, 23, 45), msmIDs = range(5001,5027)):

    timeWindow = timedelta(minutes=30)
    stats = {"measured":defaultdict(list), "inferred": defaultdict(list)}
    meanRttMeasured = defaultdict(list)
    nbSamplesMeasured = defaultdict(list)
    meanRttInferred = defaultdict(list)
    nbSamplesInferred = defaultdict(list)

    currDate = start
    while currDate+timeWindow<end:
        rttMeasured = defaultdict(list)
        rttInferred = defaultdict(list)
        sys.stderr.write("\rTesting %s " % currDate)

        for i, msmId in enumerate(msmIDs):

            if not os.path.exists("../data/%s_msmId%s.json" % (currDate, msmId)):
                continue

            fi = open("../data/%s_msmId%s.json" % (currDate, msmId) )
            data = json.load(fi)

            for trace in data:
                readOneTraceroute(trace, rttMeasured, rttInferred)

        for k, v in rttMeasured.iteritems():
            meanRttMeasured[k].append(np.median(v))
            nbSamplesMeasured[k].append(len(v))
        for k, v in rttInferred.iteritems():
            meanRttInferred[k].append(np.median(v))
            nbSamplesInferred[k].append(len(v))
            

        currDate += timeWindow
    
    sys.stderr.write("\n")
    return meanRttMeasured, meanRttInferred, nbSamplesMeasured, nbSamplesInferred



if __name__ == "__main__":
    # testDateRangeMongo(None,save_to_file=True)
    detectRttChangesMongo()
