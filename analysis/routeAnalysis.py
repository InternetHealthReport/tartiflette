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
import random
import statsmodels.api as sm
import smtplib
import emailConf
from email.mime.text import MIMEText

from bson import objectid

def sendMail(message):
    """
    Send an email with the given message.
    The destination/source addresses are defined in emailConf.
    """

    msg = MIMEText(message)
    msg["Subject"] = "Route analysis stopped on %s (UTC)!" % datetime.utcnow()
    msg["From"] = emailConf.orig 
    msg["To"] = ",".join(emailConf.dest)

    # Send the mail
    server = smtplib.SMTP(emailConf.server)
    server.starttls()
    server.login(emailConf.username, emailConf.password)
    server.sendmail(emailConf.orig, emailConf.dest, msg.as_string())
    server.quit()


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


# type needed for reference
def ddType():
    return defaultdict(float)


def routeCountRef():
    return defaultdict(ddType)


# type needed for child processes
def ddlistType():
    return defaultdict(list)


def routeCount():
    return defaultdict(ddlistType)


def readOneTraceroute(trace, routes):
    """Read a single traceroute instance and compute the corresponding routes.
    """
    
    if trace is None or not "dst_addr" in trace or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
        return 

    probeIp = trace["from"]
    dstIp = trace["dst_addr"]
    listRouter = routes[dstIp]
    prevIps = [probeIp]*3
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
                    if ip in listRouter[prevIp]:
                        listRouter[prevIp][ip].append(probeIp)
                    else:
                        listRouter[prevIp][ip] = [probeIp]

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
                projection={"result":1, "from":1, "dst_addr":1} , 
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
                    if ip1 in ip0Counter:
                        ip0Counter[ip1].extend(count)
                    else:
                        ip0Counter[ip1] = count 

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
                "minASN": 3,
                "minASNEntropy": 0.5,
                "af": "",
                "experimentDate": datetime.now(),
                "comment": "Study case for Emile (8.8.8.8) Nov. 2016",
                }

        expId = detectionExperiments.insert_one(expParam).inserted_id 
        refRoutes = defaultdict(routeCountRef)

    else:
        # Streaming mode: analyze the last time bin
        streaming = True
        now = datetime.now(timezone("UTC"))  
        expParam = detectionExperiments.find_one({"_id": expId})
        expParam["start"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC")) - timedelta(hours=1) 
        expParam["end"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC")) 
        expParam["analysisTimeUTC"] = now
        #TODO remove the 2 following lines: force minASN and entropy threshold
        expParam["minASN"]=3
        expParam["minASNEntropy"]= 0.5
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
            refRoutes = defaultdict(routeCountRef)
        sys.stderr.write("done!\n")

    probe2asn = {}
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
            params.append( (newRoutes, refRoutes[target], expParam, expId, datetime.utcfromtimestamp(currDate), target, probe2asn) )

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
        # compute magnitude
        mag, alarms = computeMagnitude(asnList, datetime.utcfromtimestamp(currDate),expId, ip2asn, alarmsCollection)
        for asn, asname in asnList:
            cursor.execute("INSERT INTO ihr_forwarding (asn_id, timebin, magnitude, resp, label) \
            VALUES (%s, %s, %s, %s, %s)", (int(asn), expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2), mag[asn], 0, "")) 
	
        conn.commit()

        # push alarms to the webserver
        ts = expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2)
        for alarm in alarms:
            if alarm["asn"] in mag:
                cursor.execute("INSERT INTO ihr_forwarding_alarms (asn_id, timebin, ip,  \
                    correlation, responsibility, pktdiff, previoushop ) VALUES (%s, %s, %s, \
                    %s, %s, %s, %s)", (alarm["asn"], ts, alarm["ip"], alarm["correlation"], alarm["responsibility"], alarm["pktDiff"], alarm["previousHop"]))

        conn.commit()
        cursor.close()
        conn.close()
        
    #print "Cleaning route change reference." 
    #refRoutes = cleanRef(refRoutes, datetime.utcfromtimestamp(currDate))


    print "Writing route change reference to file system." 
    fi = open("saved_references/%s_routeChange.pickle" % (expId), "w")
    pickle.dump(refRoutes, fi, 2) 
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    

def cleanRef(refRoutes, currDate, maxSilence=7):

    toRemove = []
    for ip, nh in refRoutes.iteritems():
        if nh["stats"]["lastSeen"] < currDate - timedelta(days=maxSilence):
            toRemove.append(ip)

    for ip in toRemove:
        del refRoutes[ip]

    print "Removed references for %s ips" % len(toRemove)

    return refRoutes

# TODO remove the following function
def repare(dt, asnList, ip2asn, expId, alarmsCollection, timeWindow=60*60):
    # update ASN table
    conn_string = "host='romain.iijlab.net' dbname='ihr'"

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # compute magnitude
    mag, alarms = computeMagnitude(asnList, dt ,expId, ip2asn, alarmsCollection)
    ts = dt+timedelta(seconds=timeWindow/2)
    for asn, asname in asnList:
        cursor.execute("INSERT INTO ihr_forwarding (asn_id, timebin, magnitude, resp, label) \
        VALUES (%s, %s, %s, %s, %s)", (int(asn), ts, mag[asn], 0, "")) 
    
    conn.commit()

    # push alarms to the webserver
    for alarm in alarms:
        if alarm["asn"] in mag:
            cursor.execute("INSERT INTO ihr_forwarding_alarms (asn_id, timebin, ip,  \
                correlation, responsibility, pktdiff, previoushop ) VALUES (%s, %s, %s, \
                %s, %s, %s, %s)", (alarm["asn"], ts, alarm["ip"], alarm["correlation"], alarm["responsibility"], alarm["pktDiff"], alarm["previousHop"]))

    conn.commit()
    cursor.close()
    conn.close()
    



def computeMagnitude(asnList, timeBin, expId, ip2asn, collection, metric="resp", 
        tau=5, historySize=7*24, minPeriods=0, corrThresh=-0.25):

    starttime = timeBin-timedelta(hours=historySize)
    alarms = []
    endtime =  timeBin
    cursor = collection.find( {
            "expId": expId,  
            "corr": {"$lt": corrThresh},
            "timeBin": {"$gt": starttime},
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
            if ip in refDict:
                sumPktDiff += np.abs(pkt - refDict[ip])
            else:
                sumPktDiff += pkt 

        for ip, pkt in obsList:
            if ip == "0" or ip == "x":
                    # data["asn"].append("Pkt.Loss")
                    # # TODO how to handle packet loss on the website ?
                continue

            if ip in refDict:
                pktDiff = pkt - refDict[ip] 
            else:
                pktDiff = pkt

            corrAbs = np.abs(row["corr"])
            data["pktDiff"].append(pktDiff)
            data["router"].append(row["ip"])
            data["timeBin"].append(row["timeBin"])
            data["ip"].append(ip)
            resp = corrAbs * (pktDiff/sumPktDiff) 
            data["resp"].append( resp )
            if not ip in ip2asn:
                ip2asn[ip] = int(asn_by_addr(ip, db=gi)[0])

            data["asn"].append(ip2asn[ip])

            if row["timeBin"] == timeBin and resp > 0.1:
                alarms.append({"asn": data["asn"][-1], "ip": ip, "previousHop": row["ip"], "correlation": row["corr"], "responsibility": resp, "pktDiff": pktDiff})

    
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

def routeChangeDetection( (routes, routesRef, param, expId, ts, target, probe2asn) ):

    collection = db.routeChanges
    alpha = param["alpha"]
    minAsn= param["minASN"]
    minASNEntropy= param["minASNEntropy"]
    minSeen = param["minSeen"]
    alarms = []
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    for ip0, nextHops in routes.iteritems(): 
        nextHopsRef = routesRef[ip0] 
        allHops = set(["0"])
        for key in set(nextHops.keys()).union([k for k, v in nextHopsRef.iteritems() if isinstance(v, float)]):
            # Make sure we don't count ip that are not observed in both variables
            if nextHops[key] or nextHopsRef[key]:
                allHops.add(key)
        
        reported = False
        if len(allHops) > 2  : 
            probes = np.array([p for hop, probeIP in nextHops.iteritems() for p in probeIP])
            hops = np.array([hop for hop, probeIP in nextHops.iteritems() for p in probeIP])
            mask = np.array([True]*len(hops))
            asn = defaultdict(int)
            asnProbeIdx = defaultdict(list)

            for idx, ip in enumerate(probes):
                if not ip in probe2asn:
                    a = asn_by_addr(ip,db=gi)
                    probe2asn[ip] = a 
                else:
                    a = probe2asn[ip]
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

            if len(asn) < minAsn or asnEntropy < minASNEntropy:
                continue

            # if trimmed then update the sample dist and probes
            if trimDist:
                hops = hops[mask]
                probes = probes[mask]

            # Refresh allHops list
            allHops = set(["0"])
            for key in set(hops).union([k for k, v in nextHopsRef.iteritems() if isinstance(v, float)]):
                # Make sure we don't count ip that are not observed in both variables
                if nextHops[key] or nextHopsRef[key]:
                    allHops.add(key)
        
            count = []
            countRef = []
            for ip1 in allHops:
                count.append(np.count_nonzero(hops == ip1))
                countRef.append(nextHopsRef[ip1])

            if len(count) > 1 and "stats" in nextHopsRef and nextHopsRef["stats"]["nbSeen"] >= minSeen:
                corr = np.corrcoef(count,countRef)[0][1]
                if corr < param["minCorr"]:
                    nbSamples = len(probes)

                    reported = True
                    alarm = {"timeBin": ts, "ip": ip0, "corr": corr, "dst_ip": target, "refNextHops": [(k, v) for k,v in nextHopsRef.iteritems()], "obsNextHops": [(k, len(v)) for k, v in nextHops.iteritems()] , "expId": expId, "nbSamples": nbSamples, "nbPeers": len(count), "nbSeen": nextHopsRef["stats"]["nbSeen"]}

                    if collection is None:
                        # Write the result to the standard output
                        print alarm 
                    else:
                        alarms.append(alarm)

            # Update the reference
            if not "stats" in nextHopsRef:
                nextHopsRef["stats"] = {"nbSeen":  0, "firstSeen": ts,
                        "lastSeen": ts, "nbReported": 0}
                #for ip1 in allHops:
                    #nextHopsRef[ip1] = []

            if reported:
                nextHopsRef["stats"]["nbReported"] += 1

            nextHopsRef["stats"]["nbSeen"] += 1
            nextHopsRef["stats"]["lastSeen"] = ts 

            #if nextHopsRef["stats"]["nbSeen"]< minSeen:                 # still in the bootstrap
                #for ip1 in allHops:
                    #newCount = np.count_nonzero(hops == ip1)
                    #if not ip1 in nextHopsRef:
                        #nextHopsRef[ip1] = []
                    #nextHopsRef[ip1].append(newCount)
            #elif nextHopsRef["stats"]["nbSeen"]== minSeen:              # end of bootstrap
                #for ip1 in allHops:
                    #newCount = np.count_nonzero(hops == ip1)
                    #if not ip1 in nextHopsRef:
                        #nextHopsRef[ip1] = []
                    #nextHopsRef[ip1].append(newCount)
                    #nextHopsRef[ip1] = float(np.median(nextHopsRef[ip1]))
            #else:
            for ip1 in allHops:
                newCount = np.count_nonzero(hops == ip1)
                if isinstance(nextHopsRef[ip1], list):
                    nextHopsRef[ip1] = float(np.median(nextHopsRef[ip1]))
                nextHopsRef[ip1] = (1.0-alpha)*nextHopsRef[ip1] + alpha*newCount 

    # Insert all alarms to the database
    if alarms and not collection is None:
        collection.insert_many(alarms)

    return (target, routesRef, alarms)


if __name__ == "__main__":
    try:
        expId = None
        if len(sys.argv)>1:
            if sys.argv[1] != "stream":
                expId = objectid.ObjectId(sys.argv[1]) 
            else:
                expId = "stream"
        detectRouteChangesMongo(expId)
    except Exception as e: 
        save_note = "Exception dump: %s : %s.\nCommand: %s" % (type(e).__name__, e, sys.argv)
        exception_fp = open("dump_%s.err" % datetime.now(), "w")
        exception_fp.write(save_note) 
        sendMail(save_note)

