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
from multiprocessing import Process, Pool
import tools
import cPickle as pickle
import pygeoip
import socket
import re
import pandas as pd
import psycopg2

from bson import objectid


asn_regex = re.compile("^AS([0-9]*)\s(.*)$")
def asn_by_addr(ip, db=None):
    try:
        m = asn_regex.match(unicode(db.asn_by_addr(ip)).encode("ascii", "ignore")) 
        if m is None:
            return ("0", "Unk")
        else:
            return m.groups() 
    except socket.error:
        return ("0", "Unk")


# type needed for child processes
def ddType():
    return defaultdict(float)

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


def countRoutes( (af, start, end) ):
    """Read traceroutes from a cursor. Used for multi-processing.
    """

    tsS = time.time()
    s = datetime.utcfromtimestamp(start)
    e = datetime.utcfromtimestamp(end)
    collectionNames = set(["traceroute%s_%s_%02d_%02d" % (af, d.year, d.month, d.day) for d in [s,e]])

    nbRow = 0
    routes = defaultdict(routeCount)
    for col in collectionNames:
        collection = db[col]
        cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
                projection={"result":1, "prb_id":1, "dst_addr":1} , 
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



def detectRouteChangesMongo(expId=None, configFile="detection.cfg"): # TODO config file implementation

    streaming = False
    nbProcesses = 12 
    binMult = 3 # number of bins = binMult*nbProcesses 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.routeExperiments
    alarmsCollection = db.routeChanges

    if expId == "stream":
        expParam = detectionExperiments.find_one({"stream": True})
        expId = expParam["_id"]

    if expId is None:
        expParam = {
                "timeWindow": 60*60, # in seconds 
                "start": datetime(2016, 11, 15, 0, 0, tzinfo=timezone("UTC")), 
                "end":   datetime(2016, 11, 26, 0, 0, tzinfo=timezone("UTC")),
                "alpha": 0.01, # parameter for exponential smoothing 
                "minCorr": -0.25, # correlation scores lower than this value will be reported
                "minSeen": 3,
                "af": "",
                "experimentDate": datetime.now(),
                "comment": "Study case for Emile (8.8.8.8) Nov. 2016",
                }

        expId = detectionExperiments.insert_one(expParam).inserted_id 
        refRoutes = defaultdict(routeCount)

    else:
        # Streaming mode: analyze the last time bin
        streaming = True
        now = datetime.now(timezone("UTC"))  
        expParam = detectionExperiments.find_one({"_id": expId})
        expParam["start"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC")) - timedelta(hours=1) 
        expParam["end"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC")) 
        expParam["analysisTimeUTC"] = now
        resUpdate = detectionExperiments.replace_one({"_id": expId}, expParam)
        if resUpdate.modified_count != 1:
            print "Problem happened when updating the experiment dates!"
            print resUpdate
            return

        sys.stderr.write("Loading previous reference...")
        try:
            fi = open("saved_references/%s_%s.pickle" % (expId, "routeChange"), "rb")
            refRoutes = pickle.load(fi) 
        except IOError:
            refRoutes = defaultdict(routeCount)
        sys.stderr.write("done!\n")

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))
    nbIteration = 0

    sys.stderr.write("Route analysis:\n")
    for currDate in range(start,end,int(expParam["timeWindow"])):
        tsS = time.time()

        # count packet routes for the current time bin
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], nbProcesses*binMult+1)
        for i in range(nbProcesses*binMult):
            params.append( (expParam["af"], binEdges[i], binEdges[i+1]) )

        nbRow = 0 
        routes =  pool.imap_unordered(countRoutes, params)
        routes, nbRow = mergeRoutes(routes, currDate, tsS, nbProcesses*binMult)

        # Detect route changes
        params = []
        for target, newRoutes in routes.iteritems():
            params.append( (newRoutes, refRoutes[target], expParam, expId, datetime.utcfromtimestamp(currDate), target ) )

        mapResult = pool.map(routeChangeDetection, params)

        # Update the reference
        for target, newRef, alarms in mapResult:
            refRoutes[target] = newRef

            
        if nbRow>0:
            nbIteration+=1


    # Update results on the webserver
    if streaming:
        # update ASN table
        conn_string = "host='romain.iijlab.net' dbname='ihr'"
 
        # get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
        cursor.execute("SELECT * FROM ihr_asn;")
        asnList = cursor.fetchall()   
 
        ip2asn = {} 
        gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
        # compute magnitude
        mag, alarms = computeMagnitude(asnList, datetime.utcfromtimestamp(currDate),expId, ip2asn, alarmsCollection)
        for asn, asname in asnList:
            cursor.execute("INSERT INTO ihr_congestion (asn_id, timebin, magnitude) \
            VALUES (%s, %s, %s)", (int(asn), expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2), mag[asn])) 

        # push alarms to the webserver
        ts = expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2)
        for alarm in alarms:
            cursor.execute("INSERT INTO ihr_forwarding_alarms (asn_id, timebin, ip,  \
                    correlation, responsibility, pktDiff, previousHop ) VALUES (%s, %s, %s, \
                    %s, %s, %s)", (int(alarm["asn"]), ts, alarm["ip"], alarm["correlation"], alarm["responsibility"],
                    alarm["pktDiff"], alarm["previousHop"]))



    for ref, label in [(sampleMediandiff, "diffRTT")]:
        if not ref is None:
            print "Writing %s reference to file system." % (label)
            fi = open("saved_references/%s_%s.pickle" % (expId, label), "w")
            pickle.dump(ref, fi, 2) 

    print "Writing route change reference to file system." 
    fi = open("saved_references/%s_routeChange.pickle" % (expId), "w")
    pickle.dump(refRoutes, fi, 2) 
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    

def computeMagnitude(asnList, timeBin, expId, ip2asn, collection, metric="resp", 
        tau=5, historySize=7*24, minPeriods=0, corrThresh=-0.25):

    starttime = timeBin-timedelta(hours=historySize)
    alarms = []
    endtime =  timeBin
    cursor = collection.find( {
            "expId": expId,  
            "corr": {"$lt": corrThresh},
            "timeBin": {"$gt": starttime, "$lte": timeBin},
            "nbPeers": {"$gt": 2},
            "nbSamples": {"$gt": 8},
        }, 
        [ 
            "obsNextHops",
            "refNextHops",
            "timeBin",
            "ip",
            "nbSamples",
            "corr",
        ],
        cursor_type=pymongo.cursor.CursorType.EXHAUST,
        batch_size=int(10e6))
    
    data = {"timeBin":[],  "router":[], "ip": [], "pktDiff": [], "resp": [], "asn": []} 
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    for i, row in enumerate(cursor):
        print "%dk \r" % (i/1000), 
        obsList = row["obsNextHops"] 
        refDict = dict(row["refNextHops"]) 
        sumPktDiff = 0
        for ip, pkt in obsList:
            sumPktDiff += np.abs(pkt - refDict[ip])

        for ip, pkt in obsList:
            if ip == "0" or ip == "x":
                    # data["asn"].append("Pkt.Loss")
                    # # TODO how to handle packet loss on the website ?
                continue

            pktDiff = pkt - refDict[ip] 

            corrAbs = np.abs(row["corr"])
            data["pktDiff"].append(pktDiff)
            data["router"].append(row["ip"])
            data["timeBin"].append(row["timeBin"])
            data["ip"].append(ip)
            resp = corrAbs * (pktDiff/sumPktDiff) 
            data["resp"].append( resp )
            if not ip in ip2asn:
                ip2asn[ip] = asn_by_addr(ip, db=gi)[0]

            data["asn"].append(ip2asn[ip])

            if row["timeBin"] == timeBin and resp > 0.1:
                alarms.append({"asn": data["asn"][-1][0], "ip": ip, "previousHop": row["ip"], "correlation": row["corr"], "responsibility": resp, "pktDiff": pktDiff})

    
    df =  pd.DataFrame.from_dict(data)
    df["timeBin"] = pd.to_datetime(df["timeBin"],utc=True)
    df.set_index("timeBin")
    
    magnitudes = {}
    for asn, asname in asnList:
        dfb = pd.DataFrame({u'resp':0.0, u'timeBin':starttime, u'asn':asn,}, index=[starttime])
        dfe = pd.DataFrame({u'resp':0.0, u'timeBin':endtime, u'asn':asn}, index=[endtime])
        dfasn = pd.concat([dfb, df[df["asn"] == asn], dfe])

        grp = dfasn.groupby("timeBin")
        grpSum = grp.sum().resample("1H").sum()
        
        mad= lambda x: np.median(np.fabs(pd.notnull(x) -np.median(pd.notnull(x))))
        magnitudes[asn] = (grpSum[metric][-1]-grpSum[metric].median()) / (1+1.4826*mad(grpSum[metric]))
    
    return magnitudes, alarms

def routeChangeDetection( (routes, routesRef, param, expId, ts, target) ):

    collection = db.routeChanges
    alpha = param["alpha"]
    alarms = []

    for ip0, nextHops in routes.iteritems(): 
        nextHopsRef = routesRef[ip0] 
        allHops = set(["0"])
        for key in set(nextHops.keys()).union([k for k, v in nextHopsRef.iteritems() if isinstance(v, float)]):
            # Make sure we don't count ip that are not observed in both variables
            if nextHops[key] or nextHopsRef[key]:
                allHops.add(key)
        
        reported = False
        nbSamples = np.sum(nextHops.values())
        nbSamplesRef = np.sum([x for x in nextHopsRef.values() if isinstance(x, float)])
        if len(allHops) > 2  and "stats" in nextHopsRef and nextHopsRef["stats"]["nbSeen"] >= param["minSeen"]: 
            count = []
            countRef = []
            avg = nbSamples 
            avgRef = nbSamplesRef 
            for ip1 in allHops:
                count.append(nextHops[ip1])
                countRef.append(nextHopsRef[ip1])

            if len(count) > 1:
                if np.std(count) == 0 or np.std(countRef) == 0:
                    print "%s, %s, %s, %s" % (allHops, countRef, count, nextHopsRef)
                corr = np.corrcoef(count,countRef)[0][1]
                if corr < param["minCorr"]:

                    reported = True
                    alarm = {"timeBin": ts, "ip": ip0, "corr": corr, "dst_ip": target,
                            "refNextHops": list(nextHopsRef.iteritems()), "obsNextHops": list(nextHops.iteritems()),
                            "expId": expId, "nbSamples": nbSamples, "nbPeers": len(count),
                            "nbSeen": nextHopsRef["stats"]["nbSeen"]}

                    if collection is None:
                        # Write the result to the standard output
                        print alarm 
                    else:
                        alarms.append(alarm)

        # Update the reference
        if not "stats" in nextHopsRef:
            nextHopsRef["stats"] = {"nbSeen":  0, "firstSeen": ts,
                    "lastSeen": ts, "nbReported": 0}

        if reported:
            nextHopsRef["stats"]["nbReported"] += 1

        nextHopsRef["stats"]["nbSeen"] += 1
        nextHopsRef["stats"]["lastSeen"] = ts 

        for ip1 in allHops:
            newCount = nextHops[ip1]
            nextHopsRef[ip1] = (1.0-alpha)*nextHopsRef[ip1] + alpha*newCount 

    # Insert all alarms to the database
    if alarms and not collection is None:
        collection.insert_many(alarms)

    return (target, routesRef, alarms)


if __name__ == "__main__":
    expId = None
    if len(sys.argv)>1:
        if sys.argv[1] != "stream":
            expId = objectid.ObjectId(sys.argv[1]) 
        else:
            expId = "stream"
    detectRouteChangesMongo(expId)
