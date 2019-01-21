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
import json
import psycopg2
import psycopg2.extras

try:
    import smtplib
    from email.mime.text import MIMEText
    import emailConf
except ImportError:
    pass
import traceback

from bson import objectid

sys.path.append("../lib/ip2asn/")
import ip2asn 

def sendMail(message):
    """
    Send an email with the given message.
    The destination/source addresses are defined in emailConf.
    """

    msg = MIMEText(message)
    msg["Subject"] = "RTT analysis stopped on %s (UTC)!" % datetime.utcnow()
    msg["From"] = emailConf.orig 
    msg["To"] = ",".join(emailConf.dest)

    # Send the mail
    server = smtplib.SMTP(emailConf.server)
    server.starttls()
    server.login(emailConf.username, emailConf.password)
    server.sendmail(emailConf.orig, emailConf.dest, msg.as_string())
    server.quit()


asn_regex = re.compile("^AS([0-9]*)\s(.*)$")
#def asn_by_addr(ip, db=None):
    #try:
        #m = asn_regex.match(unicode(db.asn_by_addr(ip)).encode("ascii", "ignore")) 
        #if m is None:
            #return ("0", "Unk")
        #else:
            #return m.groups() 
    #except socket.error:
        #return ("0", "Unk")


def readOneTraceroute(trace, diffRtt, metric=np.nanmedian):
    """Read a single traceroute instance and compute the corresponding 
    differential RTTs.
    """

    if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
        return diffRtt

    probeIp = trace["from"]
    probeId = None
    msmId = None
    if "prb_id" in trace:
        probeId = trace["prb_id"]
    if "msm_id" in trace:
        msmId = trace["msm_id"]
    prevRttMed = {}

    for hopNb, hop in enumerate(trace["result"]):
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
                            if not (ip1, ip2) in diffRtt:
                                diffRtt[(ip1,ip2)] = {"rtt": [],
                                                        "probe": [],
                                                        "msmId": defaultdict(set)
                                                        }

                            i = diffRtt[(ip1,ip2)]
                            i["rtt"].append(rttAgg-pRttAgg)
                            i["probe"].append(probeIp)
                            i["msmId"][msmId].add(probeId)
        finally:
            prevRttMed = rttMed
            # TODO we miss 2 inferred links if a router never replies

    return diffRtt



######## used by child processes
db = None

def processInit():
    global db
    client = pymongo.MongoClient("mongodb-iijlab",connect=True)
    db = client.atlas


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
                projection={"result":1, "from":1, "prb_id":1, "msm_id":1} , 
                skip = skip,
                limit = limit,
                # cursor_type=pymongo.cursor.CursorType.EXHAUST,
                batch_size=int(10e6))
        else:
            cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}, "result.result.from": prefixes } , 
                projection={"result":1, "from":1, "prb_id":1, "msm_id":1} , 
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
                        for msmId, probes in v["msmId"].iteritems():
                            inf["msmId"][msmId].update(probes)
                    else:
                        diffRtt[ipPair] = v

            nbRow += compRows
            timeSpent = (time.time()-tsS)
            if nbBins>1:
                sys.stdout.write("\r%s     [%s%s]     %.1f sec,      %.1f row/sec           " % (datetime.utcfromtimestamp(currDate),
                "#"*(30*i/(nbBins-1)), "-"*(30*(nbBins-i)/(nbBins-1)), timeSpent, float(nbRow)/timeSpent))
            else:
                sys.stdout.write("\r%s     [%s%s]     %.1f sec,      %.1f row/sec           " % (datetime.utcfromtimestamp(currDate),
                "#"*(30*i/(nbBins)), "-"*(30*(nbBins-i)/(nbBins)), timeSpent, float(nbRow)/timeSpent))

        return diffRtt, nbRow


def outlierDetection(sampleDistributions, smoothMean, param, expId, ts, probe2asn, i2a,
    collection=None, streaming=False, probeip2asn=None):

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
                a = i2a.ip2asn(ip)
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
                        if not ip in probeip2asn:
                            probeip2asn[ip] = i2a.ip2asn(ip)

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

                    nosetMsmId = {str(k): list(v) for k, v in data["msmId"].iteritems()}
                    alarm = {"timeBin": ts, "ipPair": ipPair, "currLow": currLow,"currHigh": currHi,
                            "refHigh": ref["high"], "ref":ref["mean"], "refLow":ref["low"], 
                            "median": med, "nbSamples": n, "nbProbes": nbProbes, "deviation": deviation,
                            "diff": diff, "expId": expId, "diffMed": diffMed, "samplePerASN": list(asn),
                            "nbASN": len(asn), "asnEntropy": asnEntropy, "nbSeen": ref["nbSeen"],
                            "devBound": devBound, "trimDist": trimDist, "msmId": nosetMsmId}

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


def cleanRef(ref, currDate, maxSilence=7):

    toRemove = []
    for ipPair, data in ref.iteritems():
        if data["lastSeen"] < currDate - timedelta(days=maxSilence):
            toRemove.append(ipPair)

    for ipPair in toRemove:
        del ref[ipPair]

    print "Removed references for %s ips" % len(toRemove)

    return ref


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
    df["timeBin"] = pd.to_datetime(df["timeBin"])
    df.set_index("timeBin")

    if "asn" not in df.columns:
        # find ASN for each ip
        i2a = ip2asn.ip2asn("../lib/ip2asn/db/rib.20180401.pickle", "../lib/ixs_201802.jsonl")
        fct = functools.partial(i2a.ip2asn)
        sTmp = df["ipPair"].apply(fct).apply(pd.Series)
        df["asn"] = sTmp[0]
    
    magnitudes = {}
    for asn in asnList: 

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
    replay = False
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
        if replay:
            expParam["start"]= expParam["end"]
            expParam["end"]= expParam["start"]+timedelta(hours=1)
        else:
            expParam["start"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC"))-timedelta(hours=1) 
            expParam["end"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC")) 
        expParam["analysisTimeUTC"] = now
        resUpdate = detectionExperiments.replace_one({"_id": expId}, expParam)
        if resUpdate.modified_count != 1:
            print "Problem happened when updating the experiment dates!"
            print resUpdate
            return

        sys.stdout.write("Loading previous reference...")
        try:
            fi = open("saved_references/%s_%s.pickle" % (expId, "diffRTT"), "rb")
            sampleMediandiff = pickle.load(fi) 
        except IOError:
            sampleMediandiff = {}

        sys.stdout.write("done!\n")

    if not expParam["prefixes"] is None:
        expParam["prefixes"] = re.compile(expParam["prefixes"])

    probe2asn = {}
    probeip2asn = {}
    lastAlarms = []
    i2a = ip2asn.ip2asn("../lib/ip2asn/db/rib.20180401.pickle", "../lib/ixs_201802.jsonl")

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))

    for currDate in range(start,end,int(expParam["timeWindow"])):
        sys.stdout.write("Rtt analysis %s" % datetime.utcfromtimestamp(currDate))
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
                    datetime.utcfromtimestamp(currDate), probe2asn, i2a, alarmsCollection, streaming, probeip2asn)

        timeSpent = (time.time()-tsS)
        sys.stdout.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    pool.close()
    pool.join()

    # Update results on the webserver
    if streaming:
        # update ASN table
        conn_string = "host='psqlserver' dbname='ihr'"
 
        # get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()

        asnList = set(probeip2asn.values())
        cursor.execute("SELECT number FROM ihr_asn WHERE tartiflette=TRUE")
        registeredAsn = set([x[0] for x in cursor.fetchall()])
        for asn in asnList:
            #cursor.execute("INSERT INTO ihr_asn (number, name, tartiflette) VALUES (%s, %s, %s) \
            #    ON CONFLICT (number) DO UPDATE SET tartiflette = TRUE;", (int(asn), asname, True))
            asname = i2a.asn2name(asn)
            if int(asn) not in registeredAsn:
                cursor.execute("""do $$
                begin 
                      insert into ihr_asn(number, name, tartiflette, disco, ashash) values(%s, %s, TRUE, FALSE, FALSE);
                  exception when unique_violation then
                    update ihr_asn set tartiflette = TRUE where number = %s;
                end $$;""", (asn, asname, asn))
 
        # push alarms to the webserver
        for alarm in lastAlarms:
            ts = alarm["timeBin"]+timedelta(seconds=expParam["timeWindow"]/2)
            for ip in alarm["ipPair"]:
                cursor.execute("INSERT INTO ihr_delay_alarms (asn_id, timebin, ip, link, \
                        medianrtt, nbprobes, diffmedian, deviation, msm_prb_ids) VALUES (%s, %s, %s, \
                        %s, %s, %s, %s, %s, %s) RETURNING id", (probeip2asn[ip], ts, ip, alarm["ipPair"],
                        alarm["median"], alarm["nbProbes"], alarm["diffMed"], alarm["devBound"], psycopg2.extras.Json(alarm["msmId"])))


                # Push measurement and probes ID corresponding to this alarm
                alarmid = cursor.fetchone()[0]
                for msmid, probes in alarm["msmId"].iteritems():
                    if not msmid is None:
                        for probeid in probes:
                            cursor.execute("INSERT INTO ihr_delay_alarms_msms(alarm_id, msmid, probeid) \
                                       VALUES (%s, %s, %s)", (alarmid, msmid, probeid))


        # compute magnitude
        mag = computeMagnitude(asnList, datetime.utcfromtimestamp(currDate), expId, alarmsCollection )
        for asn in asnList:
            cursor.execute("INSERT INTO ihr_delay (asn_id, timebin, magnitude, deviation, label) \
            VALUES (%s, %s, %s, %s, %s)", (asn, expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2), mag[asn], 0, "")) 

        conn.commit()
        cursor.close()
        conn.close()

        print "Cleaning rtt change reference." 
        sampleMediandiff = cleanRef(sampleMediandiff, datetime.utcfromtimestamp(currDate))

    print "Writing diffRTT reference to file system." 
    fi = open("saved_references/%s_diffRTT.pickle" % (expId), "w")
    pickle.dump(sampleMediandiff, fi, 2) 



if __name__ == "__main__":
    sys.stdout.write("Started at %s\n" % datetime.now())
    try: 
        expId = None
        if len(sys.argv)>1:
            if sys.argv[1] != "stream":
                expId = objectid.ObjectId(sys.argv[1]) 
            else:
                expId = "stream"
        detectRttChangesMongo(expId)
    except Exception as e: 
        tb = traceback.format_exc()
        save_note = "Exception dump: %s : %s.\nCommand: %s\nTraceback: %s" % (type(e).__name__, e, sys.argv, tb)
        exception_fp = open("dump_%s.err" % datetime.now(), "w")
        exception_fp.write(save_note) 
        if emailConf.dest:
            sendMail(save_note)

    sys.stdout.write("Ended at %s\n" % datetime.now())

