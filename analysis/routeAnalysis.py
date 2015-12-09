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
collection = None

def processInit():
    global collection
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    db = client.atlas
    collection = db.traceroute


def countRoutes( (start, end) ):
    """Read traceroutes from a cursor. Used for multi-processing.
    """

    tsS = time.time()
    nbRow = 0
    routes = defaultdict(routeCount)
    tsM = time.time()
    cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
            projection={"timestamp": 1, "result":1, "prb_id":1, "dst_addr":1} , 
            cursor_type=pymongo.cursor.CursorType.EXHAUST)
    tsM = time.time() - tsM
    for trace in cursor: 
        readOneTraceroute(trace, routes)
        nbRow += 1
    timeSpent = time.time()-tsS
    # print("Worker %0.1f /sec.,  mongo time: %s, total time: %s"
            # % (float(nbRow)/(timeSpent), tsM, timeSpent))

    return routes, nbRow

######## (end) used by child processes


def mergeRoutes(poolResults):

    mergedRoutes = defaultdict(routeCount)

    nbRow = 0 
    for oneProcResult, compRows in poolResults:
        for target, routes in oneProcResult.iteritems():
            for ip0, nextHops in routes.iteritems(): 
                ip0Counter = mergedRoutes[target][ip0]
                for ip1, count in nextHops.iteritems():
                    ip0Counter[ip1] += count

        nbRow += compRows

    return mergedRoutes, nbRow


def updateReference(refRoutes, newRoutes, oldRoutes):

    if oldRoutes is None:
        oldRoutes = defaultdict(routeCount)

    allTargets = set(newRoutes.keys()).union(oldRoutes.keys())

    for target in allTargets:
        
        allIp0 = set(newRoutes[target].keys()).union(oldRoutes[target].keys())
        for ip0 in allIp0:
            newRoutesIp0 = newRoutes[target][ip0]
            oldRoutesIp0 = oldRoutes[target][ip0]
            refRoutesIp0 = refRoutes[target][ip0]

            allIp1 = set(newRoutesIp0.keys()).union(oldRoutesIp0.keys())
            for ip1 in allIp1:
                refRoutesIp0[ip1] += newRoutesIp0[ip1] - oldRoutesIp0[ip1]
                assert refRoutesIp0[ip1] >= 0


def detectRouteChangesMongo(configFile="detection.cfg"): # TODO config file implementation

    nbProcesses = 4
    binMult = 10
    pool = Pool(nbProcesses,initializer=processInit) 

    expParam = {
            "timeWindow": 60*60, # in seconds
            "start": datetime(2015, 5, 31, 23, 45), 
            "end":   datetime(2015, 7, 1, 0, 0),
            "msmIDs": range(5001,5027),
            "alpha": 0.01, # significance level for the chi-square test
            "historySize": (86401/1800)*1,  # 3 days
            "experimentDate": datetime.now(),
            "minSamples": 150,
            }

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.routeExperiments
    alarmsCollection = db.routeChanges
    expId = detectionExperiments.insert_one(expParam).inserted_id 

    refRoutes = defaultdict(routeCount)
    routeHistory = deque(maxlen=expParam["historySize"])

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

        nbRow = 0 
        routes =  pool.imap_unordered(countRoutes, params)
        routes, nbRow = mergeRoutes(routes)

        if len(routeHistory) == expParam["historySize"]:
            # Detect route changes
            routeChangeDetection(routes, refRoutes, expParam, expId, 
                    datetime.fromtimestamp(currDate), alarmsCollection)
            
            # Update routes reference
            oldRoutes = routeHistory.pop()
            updateReference(refRoutes, routes, oldRoutes)
            routeHistory.appendleft(routes)

        elif nbRow > 0:
            # Still in the bootstrap
            # Update the reference 
            updateReference(refRoutes, routes, None)
            routeHistory.appendleft(routes)

        timeSpent = (time.time()-tsS)
        sys.stderr.write("Done in %s seconds,  %s row/sec\n" % (timeSpent, float(nbRow)/timeSpent))
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    

def routeChangeDetection(routesToTest, refRoutes, param, expId, ts, collection=None, updatePastData=True):

    alarms = []

    for target, routes in routesToTest.iteritems():
        routesRef = refRoutes[target]
        for ip0, nextHops in routes.iteritems(): 
            nextHopsRef = routesRef[ip0] 
            allHops = set()
            for key in set(nextHops.keys()).union(nextHopsRef.keys()):
                # Make sure we don't count ip that are not observed in both variables
                if nextHops[key] or nextHopsRef[key]:
                    allHops.add(key)
           
            nbSamples = np.sum(nextHops.values())
            if len(allHops) == 1                         # only one IP, means no change 
                or nbSamples < param["minSamples"]:      # or not enough samples
                continue
            else:
                count = []
                countRef = []
                avg = nbSamples 
                avgRef = np.sum(nextHopsRef.values())
                for ip1 in allHops:
                    if nextHops[ip1] > avg*0.25 or nextHopsRef[ip1] > avgRef*0.25: 
                        count.append(nextHops[ip1])
                        countRef.append(nextHopsRef[ip1])

                        # replace zeros in the observed data:
                        if not count[-1]:
                            count[-1]=3
                        if not countRef[-1]:
                            countRef[-1]=3

                if len(count) > 1: 
                    chi2, p, dof, _ = stats.chi2_contingency([count,countRef],correction=True)
                    if p < param["alpha"]:

                        alarm = {"timeBin": ts, "ip": ip0, "p-value": p, "dst_ip": target,
                                "refNextHops": str(nextHopsRef), "obsNextHops": str(nextHops),
                                "expId": expId}

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
