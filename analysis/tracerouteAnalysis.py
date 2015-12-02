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
from scipy import stats
import pymongo
from multiprocessing import Process, JoinableQueue, Manager
import tools

def readOneTraceroute(trace, measuredRtt, inferredRtt, metric=np.nanmedian):
    """Read a single traceroute instance and compute the corresponding 
    measured and infered RTTs.
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

                    # Infered rtt
                    if not inferredRtt is None and not ip1 is None and not np.all(np.isnan(prevRttList)) and ip1!=ip2:
                        if (ip2,ip1) in inferredRtt:
                            inferredRtt[(ip2,ip1)].append(rtt-prevRttAgg)
                        else:
                            inferredRtt[(ip1,ip2)].append(rtt-prevRttAgg)
        finally:
            prevRttList = rttList
            # TODO we miss 2 infered links if a router never replies
            if not np.all(np.isnan(prevRttList)):
                prevRttAgg = metric(prevRttList)
            ip1 = ip2

    return measuredRtt, inferredRtt

def readTracerouteQueue(queue, measuredRtts, inferredRtts, i):
    """Read traceroutes from a queue. Used for multi-processing.
    """

    while True:
        try: 
            batch = queue.get()
            tsS = time.time()
            if batch is None: 
                nbRow = 0
                break
            nbRow = len(batch)
            measuredRtt = measuredRtts[i]
            inferredRtt = inferredRtts[i]
            for trace in batch:
                readOneTraceroute(trace, measuredRtt, inferredRtt)
            measuredRtts[i] = measuredRtt
            inferredRtts[i] = inferredRtt
        finally:
            queue.task_done()
            # print("Worker %i : %0.1f /sec., dict size: (%s, %s)" % (i,float(nbRow)/(time.time()-tsS), len(measuredRtt),len(inferredRtt)))


def testDateRangeFS(g,start = datetime(2015, 5, 10, 23, 45), 
        end = datetime(2015, 5, 12, 23, 45), msmIDs = range(5001,5027)):

    timeWindow = timedelta(minutes=30)
    stats = {"measured":defaultdict(list), "infered": defaultdict(list)}
    meanRttMeasured = defaultdict(list)
    nbSamplesMeasured = defaultdict(list)
    meanRttInfered = defaultdict(list)
    nbSamplesInfered = defaultdict(list)

    currDate = start
    while currDate+timeWindow<end:
        rttMeasured = defaultdict(list)
        rttInfered = defaultdict(list)
        sys.stderr.write("\rTesting %s " % currDate)

        for i, msmId in enumerate(msmIDs):

            if not os.path.exists("../data/%s_msmId%s.json" % (currDate, msmId)):
                continue

            fi = open("../data/%s_msmId%s.json" % (currDate, msmId) )
            data = json.load(fi)

            for trace in data:
                readOneTraceroute(trace, rttMeasured, rttInfered)

        for k, v in rttMeasured.iteritems():
            meanRttMeasured[k].append(np.median(v))
            nbSamplesMeasured[k].append(len(v))
        for k, v in rttInfered.iteritems():
            meanRttInfered[k].append(np.median(v))
            nbSamplesInfered[k].append(len(v))
            

        currDate += timeWindow
    
    sys.stderr.write("\n")
    return meanRttMeasured, meanRttInfered, nbSamplesMeasured, nbSamplesInfered

def testDateRangeMongo(g,start = datetime(2015, 2, 1, 23, 45), 
        end = datetime(2015, 2, 2, 23, 45), msmIDs = range(5001,5027)):

    nbProcesses = 4
    batchSize = 5000
    manager = Manager()
    measuredRtts = manager.list()
    inferredRtts = manager.list()
    tracerouteQueue = JoinableQueue()
    proc = []
    for i in range(nbProcesses):
        measuredRtts.append(defaultdict(list))
        inferredRtts.append(defaultdict(list))
        proc.append(Process(target=readTracerouteQueue, args=(tracerouteQueue, measuredRtts, inferredRtts, i)))
        proc[i].start()

    timeWindow = 30*60  # 30 minutes
    meanRttMeasured = defaultdict(list)
    nbSamplesMeasured = defaultdict(list)
    meanRttInfered = defaultdict(list)
    nbSamplesInfered = defaultdict(list)

    client = pymongo.MongoClient("mongodb-iijlab", connect=True)
    db = client.atlas
    collection = db.traceroute

    start = time.mktime(start.timetuple())
    end = time.mktime(end.timetuple())

    currDate = start
    batch = np.array([None]*batchSize) 
    tsS = time.time()
    nbRow = 0
    nbRowTotal = 0
    for trace in collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
            projection={"timestamp": 1, "result":1, "prb_id":1} , 
            sort=[("timestamp", pymongo.ASCENDING)],
            cursor_type=pymongo.cursor.CursorType.EXHAUST):
        if trace["timestamp"] > currDate+timeWindow:

            tracerouteQueue.put(batch)
            sys.stderr.write(" Waiting for workers... queue size = %s, %s/sec\n" % (tracerouteQueue.qsize(), float(nbRowTotal)/(time.time()-tsS)))
            tsS = time.time()
            batch = np.array([None]*batchSize) 
            nbRow = 0
            nbRowTotal = 0
            tracerouteQueue.join() 


            sys.stderr.write(" Merging results...\n")
            measuredRtt = defaultdict(list)
            for mRtt in measuredRtts:
                for k, v in mRtt.iteritems():
                    measuredRtt[k].extend(v)

            inferredRtt = defaultdict(list)
            for iRtt in inferredRtts:
                for k, v in iRtt.iteritems():
                    inferredRtt[k].extend(v)
                    
            for i in range(nbProcesses):
                mRtt = measuredRtts[i]
                iRtt = inferredRtts[i]
                mRtt.clear()
                iRtt.clear()
                measuredRtts[i] = mRtt
                inferredRtts[i] = iRtt

            for k, v in measuredRtt.iteritems():
                meanRttMeasured[k].append(np.median(v))
                nbSamplesMeasured[k].append(len(v))
            for k, v in inferredRtt.iteritems():
                meanRttInfered[k].append(np.median(v))
                nbSamplesInfered[k].append(len(v))

            currDate += timeWindow
            sys.stderr.write("\rTesting %s " % currDate)

        batch[nbRow] = trace
        nbRow += 1
        nbRowTotal += 1
        if nbRow == batchSize:
            tracerouteQueue.put(batch)
            batch = np.array([None]*batchSize)
            nbRow = 0
            
        # readOneTraceroute(trace, rttMeasured, rttInfered)
    
    sys.stderr.write("\n")
    for _ in range(nbProcesses):
        tracerouteQueue.put(None)

    for i in range(nbProcesses):
        proc[i].join()

    tracerouteQueue.close()

    return meanRttMeasured, meanRttInfered, nbSamplesMeasured, nbSamplesInfered

if __name__ == "__main__":
    testDateRangeMongo(None)
