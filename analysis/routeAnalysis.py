import sys
import itertools
from datetime import datetime
from datetime import timedelta
from pytz import timezone
import calendar
import time
import os
import logging
import json
import glob
import numpy as np
from collections import defaultdict
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from collections import deque
from scipy import stats
import pymongo
import msgpack
from multiprocessing import Process, Pool
from multiprocessing import Manager
import tools
import pickle
import pygeoip
import socket
import re
import pandas as pd
import psycopg2
import psycopg2.extras
import random
import statsmodels.api as sm
try:
    import smtplib
    from configs import emailConf
    from email.mime.text import MIMEText
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
    Mainly used to track when something wrong happens.
    """

    msg = MIMEText(message)
    msg["Subject"] = "Route analysis stopped on %s (UTC)!" % datetime.utcnow()
    msg["From"] = emailConf.orig 
    msg["To"] = ",".join(emailConf.dest)

    # Send the mail
    server = smtplib.SMTP(emailConf.server,port=587)
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
def routeCount():
    return defaultdict(routeCountRef)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass

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
    if "prb_id" in trace:
        probeId = trace["prb_id"]
    if "msm_id" in trace:
        msmId = trace["msm_id"]

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

                nbPrevIps = float(len(prevIps))
                for prevIp in prevIps:
                    if not ip in listRouter[prevIp]:
                        listRouter[prevIp][ip] = defaultdict(int) 
                        listRouter[prevIp][ip]["msm"] = defaultdict(set)
                    listRouter[prevIp][ip][probeIp] += 1/nbPrevIps
                    listRouter[prevIp][ip]["msm"][msmId].add(probeId)


                currIps.append(ip)
    
        if currIps:
            prevIps = currIps
            currIps = []

######## used by child processes
db = None
i2a = None

def processInit():
    global db
    global i2a
    # client = pymongo.MongoClient("mongodb://127.0.0.1:27017",username="mongo",password="mongo")
    # db = client.atlas
    i2a = ip2asn.ip2asn("../lib/ip2asn/db/rib.20180401.pickle.bz2")


def countRoutes(args):
    """Read traceroutes from a cursor. Used for multi-processing.
    """
    af, start, end = args

    tsS = time.time()
    s = datetime.utcfromtimestamp(start)
    e = datetime.utcfromtimestamp(end)
    collectionNames = set(["traceroute%s_%s_%02d_%02d" % (af, d.year, d.month, d.day) for d in [s,e]])

    nbRow = 0
    routes = defaultdict(routeCount)
    for col in collectionNames:
        collection = db[col]
        cursor = collection.find( { "timestamp": {"$gte": start, "$lt": end}} , 
                projection={"result":1, "from":1, "dst_addr":1, "prb_id":1, "msm_id":1} , 
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
        for target, routes in oneProcResult.items():
            for ip0, nextHops in routes.items(): 
                ip0Counter = mergedRoutes[target][ip0]
                for ip1, probeCounts in nextHops.items():
                    if ip1 in ip0Counter:
                        for probeIp, count in probeCounts.items():
                            if probeIp == "msm":
                                for msmId, probes in count.items():
                                    ip0Counter[ip1]["msm"][msmId].update(probes)
                            else:
                                ip0Counter[ip1][probeIp] += count 
                    else:
                        ip0Counter[ip1] = probeCounts 

        nbRow += compRows
        timeSpent = (time.time()-tsS)
        
        print("\r%s     [%s%s]     %.1f sec,      %.1f row/sec           " % (datetime.utcfromtimestamp(currDate),
            "#"*int((30*i/(nbBins-1))), "-"*int((30*(nbBins-i)/(nbBins-1))), timeSpent, float(nbRow)/timeSpent))

    return mergedRoutes, nbRow



def detectRouteChangesMongo(expId=None, configFile="detection.cfg"): # TODO config file implementation
    global producer
    global created_topics
    streaming = False
    replay = False
    nbProcesses = 18
    binMult = 3 # number of bins = binMult*nbProcesses 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)
    topics = ["routeExperiments", "routeChanges"]
    
    if not set(topics).issubset(set(created_topics)):
        topic_list = [NewTopic(topic, num_partitions=3, replication_factor=2) for topic in topics]
        created_topic = admin_client.create_topics(topic_list)
        for topic, f in created_topic.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
        created_topics.append(topic)
        
    refRoutes=None

    # if expId == "stream":
    #     expParam = detectionExperiments.find_one({"stream": True})
    #     expId = expParam["_id"]

    if expId is None:
        expParam = {
                "timeWindow": 60*60, # in seconds 
                "start": datetime(2016, 11, 15, 0, 0, tzinfo=timezone("UTC")).strftime('%Y-%m-%dT%H:%M:%SZ'), 
                "end":   datetime(2016, 11, 26, 0, 0, tzinfo=timezone("UTC")).strftime('%Y-%m-%dT%H:%M:%SZ'),
                "alpha": 0.01, # parameter for exponential smoothing 
                "minCorr": -0.25, # correlation scores lower than this value will be reported
                "minSeen": 3,
                "minASN": 3,
                "minASNEntropy": 0.5,
                "af": "",
                "experimentDate": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "comment": "Study case for Emile (8.8.8.8) Nov. 2016",
                }
        try:
            logging.debug('going to produce something')
            expId = producer.produce(
                topics[0], 
                msgpack.packb(expParam, use_bin_type=True),
                callback=delivery_report,
            )
            logging.debug('produced something')
            producer.poll(0)
        except BufferError:
            logging.error('Local queue is full ')
            producer.flush()
            expId = producer.produce(
                topics[0], 
                msgpack.packb(expParam, use_bin_type=True),
                callback=delivery_report,
            )
            producer.poll(0)
        producer.flush()           
        sampleMediandiff = {}
        refRoutes = defaultdict(routeCountRef)

    else:
        # Streaming mode: analyze the last time bin
        streaming = True
        now = datetime.now(timezone("UTC"))  
        # expParam = detectionExperiments.find_one({"_id": expId})
        # if replay:
        #     expParam["start"]= expParam["end"]
        #     expParam["end"]= expParam["start"]+timedelta(hours=1)
        # else:
        #     expParam["start"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC"))-timedelta(hours=1) 
        #     expParam["end"]= datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone("UTC")) 
        # expParam["analysisTimeUTC"] = now
        # expParam["minASN"]=3
        # expParam["minASNEntropy"]= 0.5
        # resUpdate = detectionExperiments.replace_one({"_id": expId}, expParam)
        # if resUpdate.modified_count != 1:
        #     print("Problem happened when updating the experiment dates!")
        #     print(resUpdate)
        #     return

        print("Loading previous reference...")
        try:
            fi = open("saved_references/%s_%s.pickle" % (expId, "routeChange"), "rb")
            refRoutes = pickle.load(fi) 
        except IOError:
            print("corrupted file!?")
            refRoutes = defaultdict(routeCountRef)
        print("done!\n")

    probe2asn = {}
    start_time = datetime.strptime(expParam["start"], '%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.strptime(expParam["end"], '%Y-%m-%dT%H:%M:%SZ')

    start = int(calendar.timegm(start_time.timetuple()))
    end = int(calendar.timegm(end_time.timetuple()))
    nbIteration = 0

    print("Route analysis:\n")
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

        print("size before params: %s" % len(refRoutes))
        # Detect route changes
        params = []
        for target, newRoutes in routes.items():
            params.append( (newRoutes, refRoutes[target], expParam, expId, datetime.utcfromtimestamp(currDate), target, probe2asn ) )

        print("size after params: %s" % len(refRoutes))

        mapResult = pool.imap_unordered(routeChangeDetection, params)

        # Update the reference
        for target, newRef in mapResult:
            refRoutes[target] = newRef

        print("size after analysis: %s" % len(refRoutes))
            
        if nbRow>0:
            nbIteration+=1


    # Update results on the webserver
    if streaming:
        i2a = ip2asn.ip2asn("../lib/ip2asn/db/rib.20180401.pickle.bz2")
        # update ASN table
        conn_string = "host='psqlserver' dbname='ihr'"
 
        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("SELECT number, name FROM ihr_asn WHERE tartiflette=TRUE;")
        asnList = cursor.fetchall()   
 
        probeip2asn = {} 
        # compute magnitude
        mag, alarms = computeMagnitude(asnList, datetime.utcfromtimestamp(currDate),expId, probeip2asn, alarmsCollection, i2a)
        for asn, asname in asnList:
            cursor.execute("INSERT INTO ihr_forwarding (asn_id, timebin, magnitude) \
            VALUES (%s, %s, %s)", (int(asn), expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2), mag[asn])) 
	
        conn.commit()

        # push alarms to the webserver
        ts = expParam["start"]+timedelta(seconds=expParam["timeWindow"]/2)
        for alarm in alarms:
            if alarm["asn"] in mag:
                cursor.execute("INSERT INTO ihr_forwarding_alarms (asn_id, timebin, ip,  \
                    correlation, responsibility, pktdiff, previoushop, msm_prb_ids ) VALUES (%s, %s, %s, \
                    %s, %s, %s, %s, %s) RETURNING id", 
                    (alarm["asn"], ts, alarm["ip"], alarm["correlation"], alarm["responsibility"], 
                        alarm["pktDiff"], alarm["previousHop"], psycopg2.extras.Json(alarm["msmId"])))

                # # Push measurement and probes ID corresponding to this alarm
                # alarmid = cursor.fetchone()[0]
                # for msmid, probes in alarm["msmId"].items():
                    # if not msmid is None:
                        # for probeid in probes:
                            # cursor.execute("INSERT INTO ihr_forwarding_alarms_msms(alarm_id, msmid, probeid) \
                                   # VALUES (%s, %s, %s)", (alarmid, msmid, probeid))

        conn.commit()
        cursor.close()
        conn.close()
        

    pool.close()
    pool.join()
    
    print("\n")
    print("Writing route change reference to file system." )
    fi = open("saved_references/%s_routeChange.pickle" % (expId), "wb")
    pickle.dump(refRoutes, fi, 2) 
    

def computeMagnitude(asnList, timeBin, expId, probeip2asn, collection, i2a, metric="resp", 
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
            "msmId",
        ],
        cursor_type=pymongo.cursor.CursorType.EXHAUST,
        batch_size=int(10e6))
    
    data = {"timeBin":[],  "router":[], "ip": [], "pktDiff": [], "resp": [], "asn": []} 

    for i, row in enumerate(cursor):
        print("\r %dk" % (i/1000),)
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
            if not ip in probeip2asn:
                probeip2asn[ip] = int(i2a.ip2asn(ip))

            data["asn"].append(probeip2asn[ip])

            if row["timeBin"] == timeBin and resp > 0.1:
                alarms.append({"asn": data["asn"][-1], "ip": ip, "previousHop": row["ip"], "correlation": row["corr"], "responsibility": resp, "pktDiff": pktDiff, "msmId": row["msmId"][ip.replace(".","_")]})

    
    df =  pd.DataFrame.from_dict(data)
    df["timeBin"] = pd.to_datetime(df["timeBin"])
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

def routeChangeDetection(args):
    global producer
    routes, rr, param, expId, ts, target, probe2asn = args
    collection = None
    alpha = param["alpha"]
    minAsn= param["minASN"]
    minASNEntropy= param["minASNEntropy"]
    minSeen = param["minSeen"]
    alarms = []

    for ip0, nextHops in routes.items(): 
        nextHopsRef = rr[ip0] 
        allHops = set(["0"])
        for key in set(nextHops.keys()).union([k for k, v in nextHopsRef.items() if isinstance(v, float)]):
            # Make sure we don't count ip that are not observed in both variables
            if nextHops[key] or nextHopsRef[key]:
                allHops.add(key)
        
        reported = False
        # TODO change the following using dict instead of lists ?
        probes = np.array([p for hop, probeCount in nextHops.items() for p, count in probeCount.items() if isinstance(count, float) for x in range(int(count))])
        hops = np.array([hop for hop, probeCount in nextHops.items() for p, count in probeCount.items() if isinstance(count, float) for x in range(int(count))])
        mask = np.array([True]*len(hops))
        asn = defaultdict(int)
        asnProbeIdx = defaultdict(list)


        for idx, ip in enumerate(probes):
            if not ip in probe2asn:
                a = i2a.ip2asn(ip)
                probe2asn[ip] = a 
            else:
                a = probe2asn[ip]
            asn[a] += 1
            asnProbeIdx[a].append(idx)

        if len(asn) < minAsn :
            continue
                
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
        for key in set(hops).union([k for k, v in nextHopsRef.items() if isinstance(v, float)]):
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
                msmIds = {hop.replace(".","_"):{str(msmid):list(probes)} for hop in nextHops.iterkeys() if "msm" in nextHops[hop] for msmid, probes in nextHops[hop]["msm"].items()}
                alarm = {"timeBin": ts, "ip": ip0, "corr": corr, "dst_ip": target, "refNextHops": [(k, v) for k,v in nextHopsRef.items()], "obsNextHops": [(k, np.sum([x for x in v.values() if isinstance(x, float)])) for k, v in nextHops.items()] , "expId": expId, "nbSamples": nbSamples, "nbPeers": len(count), "nbSeen": nextHopsRef["stats"]["nbSeen"], "msmId": msmIds}

                if collection is None:
                    # Write the result to the standard output
                    print(alarm)
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

    # Clean the reference data structure:
    toRemove = []
    for ip0, nextHopsRef in rr.items(): 
        if len(nextHopsRef) == 0:
            # there should be at least the "stats" and one hop
            toRemove.append(ip0)
        elif nextHopsRef["stats"]["lastSeen"] < ts - timedelta(days=7):
            toRemove.append(ip0)

    for ip in toRemove:
        del rr[ip] 

    # Insert all alarms to the database
    topic_name = "alarms"
    if alarms:
        for alarm in alarms:
            try:
                logging.debug('going to produce something')
                expId = producer.produce(
                    topic_name, 
                    msgpack.packb(alarm, use_bin_type=True),
                    callback=delivery_report,
                )
                logging.debug('produced something')
                producer.poll(0)
            except BufferError:
                logging.error('Local queue is full ')
                producer.flush()
                expId = producer.produce(
                    topic_name, 
                    msgpack.packb(alarm, use_bin_type=True),
                    callback=delivery_report,
                )
                producer.poll(0)
            producer.flush()      
            

    return target, rr

if __name__ == "__main__":
    print("Started at %s\n" % datetime.now())
    
    # logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, filename='../logs/route-Analysis.log' , 
            level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)
    
    producer = Producer({'bootstrap.servers':"kafka1:9092,kafka2:9092,kafka3:9092",
            'queue.buffering.max.messages': 10000000,
            'queue.buffering.max.kbytes': 2097151,
            'linger.ms': 200,
            'batch.num.messages': 1000000,
            'message.max.bytes': 999000,
            'default.topic.config': {'compression.codec': 'snappy'}})    
    admin_client = AdminClient({'bootstrap.servers':"kafka1:9092,kafka2:9092,kafka3:9092"})
    
    created_topics = list(admin_client.list_topics(timeout=5).topics.keys())
    
    consumer = Consumer({
        'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
        'group.id': "rtt-analysis-consumer-group",
        'auto.offset.reset': 'earliest',
        })
    
    try:
        expId = None
        if len(sys.argv)>1:
            if sys.argv[1] != "stream":
                expId = objectid.ObjectId(sys.argv[1]) 
            else:
                expId = "stream"
        detectRouteChangesMongo(expId)
    except Exception as e: 
        tb = traceback.format_exc()
        save_note = "Exception dump: %s : %s.\nCommand: %s\nTraceback: %s" % (type(e).__name__, e, sys.argv, tb)
        exception_fp = open("../logs/routeAnalysis_dump_%s.err" % datetime.now(), "w")
        exception_fp.write(save_note) 
        if emailConf.dest:
            sendMail(save_note)

    print("Ended at %s\n" % datetime.now())

