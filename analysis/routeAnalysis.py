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
import pykov


# type needed for child processes
def ddType():
    return defaultdict(int)

def routeCount():
    return defaultdict(ddType)


def readOneTraceroute(trace, routes):
    """Read a single traceroute instance and compute the corresponding routes.
    """
    
    # TODO verify that error doesn't mean packet lost
    if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
        return 

    ipProbe = "probe_%s"  % trace["prb_id"]
    dstIp = trace["dst_addr"]
    listRouter = routes[dstIp]
    prevIps = [ipProbe]*3
    currIps = []

    for hop in trace["result"]:
        
        if "result" in hop :
            for resNb, res in enumerate(hop["result"]):
                # In case of routers not sending ICMP or packet loss, result
                # looks like: 
                # {u'result': [{u'x': u'*'}, {u'x': u'*'}, {u'x': u'*'}], u'hop': 6}, 
                if not "from" in res:
                    ip = "x"
                elif tools.isPrivateIP(res["from"]):
                    continue
                else:
                    ip = res["from"]

                for prevIp in prevIps:
                    listRouter[prevIp][ip] += 1

                currIps.append(ip)
    
        if currIps:
            prevIps = currIps
            currIps = []

######## used by child processes
db = None

def processInit():
    global db
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    db = client.atlas


def countRoutes( (start, end) ):
    """Read traceroutes from a cursor. Used for multi-processing.
    """

    tsS = time.time()
    s = datetime.utcfromtimestamp(start)
    e = datetime.utcfromtimestamp(end)
    collectionNames = set(["traceroute_%s_%02d_%02d" % (d.year, d.month, d.day) for d in [s,e]])

    nbRow = 0
    routes = defaultdict(routeCount)
    for col in collectionNames:
        collection = db[col]
        cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
                projection={"timestamp": 1, "result":1, "prb_id":1, "dst_addr":1} , 
                cursor_type=pymongo.cursor.CursorType.EXHAUST,
                batch_size=int(10e6))
        for trace in cursor: 
            readOneTraceroute(trace, routes)
            nbRow += 1

    return routes, nbRow

######## (end) used by child processes


def mergeRoutes(poolResults, currDate, tsS, nbBins):

    mergedRoutes = defaultdict(routeCount)

    nbRow = 0 
    for i, (oneProcResult, compRows) in enumerate(poolResults):
        for target, routes in oneProcResult.iteritems():
            for ip0, nextHops in routes.iteritems(): 
                ip0Counter = mergedRoutes[target][ip0]
                for ip1, count in nextHops.iteritems():
                    ip0Counter[ip1] += count

        nbRow += compRows
        timeSpent = (time.time()-tsS)
        sys.stderr.write("\r%s     [%s%s]     %.1f sec,      %.1f row/sec           " % (datetime.utcfromtimestamp(currDate),
            "#"*(30*i/(nbBins-1)), "-"*(30*(nbBins-i)/(nbBins-1)), timeSpent, float(nbRow)/timeSpent))

    return mergedRoutes, nbRow


def updateReference(refRoutes, newRoutes, alpha, minSamples):

    for target, targetRoutes in newRoutes.iteritems():

        for ip0, newRoutesIp0 in targetRoutes.iteritems():
            nbSamples = np.sum(newRoutesIp0.values())
            if nbSamples < minSamples:
                continue
            refRoutesIp0 = refRoutes[target][ip0]
            allPeers = set(refRoutesIp0.keys()).union(newRoutesIp0.keys())

            for ip1 in allPeers:
                newCount = newRoutesIp0[ip1]
                refRoutesIp0[ip1] = alpha*newCount + (1.0-alpha)*refRoutesIp0[ip1] 
                assert refRoutesIp0[ip1] >= 0


def detectRouteChangesMongo(configFile="detection.cfg"): # TODO config file implementation

    nbProcesses = 6
    binMult = 5 
    pool = Pool(nbProcesses,initializer=processInit) 

    expParam = {
            "timeWindow": 60*60, # in seconds
            "start": datetime(2015, 5, 31, 23, 45, tzinfo=timezone("UTC")), 
            "end":   datetime(2015, 12, 22, 0, 0, tzinfo=timezone("UTC")),
            "msmIDs": range(5001,5027),
            "alpha": 0.01, # parameter for exponential smoothing 
            "minCorr": 0.25, # correlation lower than this value will be reported
            "experimentDate": datetime.now(),
            "minSamples": 25,
            "comment": "use absolute number of packets",
            }

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.routeExperiments
    alarmsCollection = db.routeChanges
    expId = detectionExperiments.insert_one(expParam).inserted_id 

    refRoutes = defaultdict(routeCount)

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))
    nbIteration = 0

    sys.stderr.write("Route analysis:\n")
    for currDate in range(start,end,expParam["timeWindow"]):
        tsS = time.time()

        # count packet routes for the current time bin
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (binEdges[i], binEdges[i+1]) )

        nbRow = 0 
        routes =  pool.imap_unordered(countRoutes, params)
        routes, nbRow = mergeRoutes(routes, currDate, tsS, nbProcesses*binMult)

        # Detect route changes
        routeChangeDetection(routes, refRoutes, expParam, expId, 
                datetime.utcfromtimestamp(currDate), alarmsCollection)
            
        # Update routes reference
        updateReference(refRoutes, routes, expParam["alpha"], 0) #expParam["minSamples"])

        if nbRow>0:
            nbIteration+=1

    print "Writing route change reference to file system." 
    fi = open("saved_references/%s_routeChange.pickle" % (expId), "w")
    pickle.dump(refRoutes, fi, 2) 
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    

def routeChangeDetection(routesToTest, refRoutes, param, expId, ts, collection=None, updatePastData=True):

    alarms = []

    for target, routes in routesToTest.iteritems():
        routesRef = refRoutes[target]
        for ip0, nextHops in routes.iteritems(): 
            nextHopsRef = routesRef[ip0] 
            allHops = set(["0"])
            for key in set(nextHops.keys()).union(nextHopsRef.keys()):
                # Make sure we don't count ip that are not observed in both variables
                if nextHops[key] or nextHopsRef[key]:
                    allHops.add(key)
           
            nbSamples = np.sum(nextHops.values())
            nbSamplesRef = np.sum(nextHopsRef.values())
            if len(allHops) == 2  or nbSamples < param["minSamples"] or nbSamplesRef < param["minSamples"]:                        
                # only one IP (means no route change) or not enough samples
                continue
            else:
                count = []
                countRef = []
                avg = nbSamples 
                avgRef = nbSamplesRef 
                for ip1 in allHops:
                    count.append(nextHops[ip1])
                    countRef.append(nextHopsRef[ip1])

                if len(count) > 1: 
                    corr = np.corrcoef(count,countRef)[0][1]
                    if corr < param["minCorr"]:

                        alarm = {"timeBin": ts, "ip": ip0, "corr": corr, "dst_ip": target,
                                "refNextHops": str(nextHopsRef), "obsNextHops": str(nextHops),
                                "expId": expId, "nbSamples": nbSamples, "nbPeers": len(count)}

                        if collection is None:
                            # Write the result to the standard output
                            print alarm 
                        else:
                            alarms.append(alarm)

    # Insert all alarms to the database
    if alarms and not collection is None:
        collection.insert_many(alarms)


if __name__ == "__main__":
    # testDateRangeMongo(None,save_to_file=True)
    detectRouteChangesMongo()
