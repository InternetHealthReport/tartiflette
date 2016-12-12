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
import random
import re
import psycopg2


# import cProfile

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


def outlierDetection(sampleDistributions, smoothMean, param, expId, ts, probe2asn, gi,
    collection=None, streaming=False, ip2asn=None):

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
    
            if ref["nbSeen"] >= minSeen:
                if streaming:
                    for ip in ipPair:
                        if not ip in ip2asn:
                            ip2asn[ip] = asn_by_addr(ip, db=gi)

                if (ref["high"] < currLow or ref["low"] > currHi):
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
                ref["mean"].append(med)
                ref["high"].append(currHi)
                ref["low"].append(currLow)
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

    return alarms

def computeMagnitude(asnList, timebin, expId, collection, tau=5, metric="devBound",
        historySize=7*24, minPeriods=0):

    # Retrieve alarms
    starttime = timebin-timedelta(hours=historySize)
    endtime =  timebin
    cursor = collection.aggregate([
        {"$match": {
            "expId": expId, 
            "timeBin": {"$gt": starttime, "$lte": timebin},
            "diffMed": {"$gt": 1},
            }}, 
        {"$project": {
            "ipPair":1,
            "timeBin":1,
            "devBound": 1,
            }},
        {"$unwind": "$ipPair"},
        ])

    df =  pd.DataFrame(list(cursor))
    df["timeBin"] = pd.to_datetime(df["timeBin"],utc=True)
    df.set_index("timeBin")

    if "asn" not in df.columns:
        # find ASN for each ip
        ga = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
        fct = functools.partial(asn_by_addr, db=ga)
        sTmp = df["ipPair"].apply(fct).apply(pd.Series)
        df["asn"] = sTmp[0]
    
    magnitudes = {}
    for asn, asname in asnList: 

        dfb = pd.DataFrame({u'devBound':0.0, u'timeBin':starttime, u'asn':asn,}, index=[starttime])
        dfe = pd.DataFrame({u'devBound':0.0, u'timeBin':endtime, u'asn':asn}, index=[endtime])
        dfasn = pd.concat([dfb, df[df["asn"] == asn], dfe])

        grp = dfasn.groupby("timeBin")
        grpSum = grp.sum().resample("1H").sum()

        mad= lambda x: np.median(np.fabs(pd.notnull(x) -np.median(pd.notnull(x))))
        magnitudes[asn] = (grpSum[metric][-1]-grpSum[metric].median()) / (1+1.4826*mad(grpSum[metric]))

    return magnitudes


def detectRttChangesMongo(expId=None):

    streaming = False
    nbProcesses = 12 
    binMult = 3 # number of bins = binMult*nbProcesses 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.rttExperiments
    alarmsCollection = db.rttChanges

    if expId == "stream":
        expParam = detectionExperiments.find_one({"stream": True})
        expId = expParam["_id"]


    if expId is None:
        expParam = {
                "timeWindow": 60*60, # in seconds 
                "start": datetime(2016, 11, 1, 0, 0, tzinfo=timezone("UTC")), 
                "end":   datetime(2016, 11, 26, 0, 0, tzinfo=timezone("UTC")),
                "alpha": 0.01, 
                "confInterval": 0.05,
                "minASN": 3,
                "minASNEntropy": 0.5,
                "minSeen": 3,
                "experimentDate": datetime.now(),
                "af": "",
                "comment": "Study case for Emile (8.8.8.8) Nov. 2016",
                "prefixes": None
                }

        expId = detectionExperiments.insert_one(expParam).inserted_id 
        sampleMediandiff = {}

    else:
        # streaming mode: analyze what happened in the last time bin
        streaming = True
        now = datetime.now(timezone("UTC"))  
        expParam = detectionExperiments.find_one({"_id": expId})
        expParam["start"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC"))-timedelta(hours=1) 
        expParam["end"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC")) 
        expParam["analysisTimeUTC"] = now
        resUpdate = detectionExperiments.replace_one({"_id": expId}, expParam)
        if resUpdate.modified_count != 1:
            print "Problem happened when updating the experiment dates!"
            print resUpdate
            return

        sys.stderr.write("Loading previous reference...")
        try:
            fi = open("saved_references/%s_%s.pickle" % (expId, "diffRTT"), "rb")
            sampleMediandiff = pickle.load(fi) 
        except IOError:
            sampleMediandiff = {}

        sys.stderr.write("done!\n")

    if not expParam["prefixes"] is None:
        expParam["prefixes"] = re.compile(expParam["prefixes"])

    probe2asn = {}
    ip2asn = {}
    lastAlarms = []
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))

    for currDate in range(start,end,int(expParam["timeWindow"])):
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
        lastAlarms = outlierDetection(diffRtt, sampleMediandiff, expParam, expId, 
                    datetime.utcfromtimestamp(currDate), probe2asn, gi, alarmsCollection, streaming, ip2asn)

        timeSpent = (time.time()-tsS)
        sys.stderr.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    pool.close()
    pool.join()

    # Update results on the webserver
    if streaming:
        # update ASN table
        conn_string = "host='romain.iijlab.net' dbname='ihr'"
 
        # get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()

        asnList = set(ip2asn.values())
        for asn, asname in asnList:
            cursor.execute("INSERT INTO ihr_asn (number, name) SELECT %s, %s \
                WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number= %s);", (int(asn), asname, asn))
 
        # push alarms to the webserver
        for alarm in lastAlarms:
            ts = alarm["timeBin"]+timedelta(seconds=expParam["timeWindow"]/2)
            for ip in alarm["ipPair"]:
                cursor.execute("INSERT INTO ihr_congestion_alarms (asn_id, timebin, ip, link, \
                        medianrtt, nbprobes, diffmedian, deviation) VALUES (%s, %s, %s, \
                        %s, %s, %s, %s, %s)", (ip2asn[ip][0], ts, ip, alarm["ipPair"],
                        alarm["median"], alarm["nbProbes"], alarm["diffMed"], alarm["devBound"]))

        # compute magnitude
        mag = computeMagnitude(asnList, datetime.utcfromtimestamp(currDate), expId, alarmsCollection )
        for asn, asname in asnList:
            cursor.execute("INSERT INTO ihr_congestion (asn_id, timebin, magnitude, deviation, label) \
            VALUES (%s, %s, %s, %s, %s)", (asn, expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2), mag[asn], 0, "")) 

        conn.commit()
        cursor.close()
        conn.close()


    for ref, label in [(sampleMediandiff, "diffRTT")]:
        if not ref is None:
            print "Writing %s reference to file system." % (label)
            fi = open("saved_references/%s_%s.pickle" % (expId, label), "w")
            pickle.dump(ref, fi, 2) 


if __name__ == "__main__":
    expId = None
    if len(sys.argv)>1:
	if sys.argv[1] != "stream":
            expId = objectid.ObjectId(sys.argv[1]) 
	else:
            expId = "stream"
    detectRttChangesMongo(expId)

