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
# import pykov
import cPickle as pickle

from bson import objectid


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


    nbProcesses = 12 
    binMult = 3 # number of bins = binMult*nbProcesses 
    pool = Pool(nbProcesses,initializer=processInit) #, maxtasksperchild=binMult)

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.routeExperiments

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
            params.append( (newRoutes, refRoutes[target], expParam, expId, datetime.utcfromtimestamp(currDate), target) )

        mapResult = pool.map(routeChangeDetection, params)

        # Update the reference
        for target, newRef in mapResult:
            refRoutes[target] = newRef

            
        if nbRow>0:
            nbIteration+=1

    print "Writing route change reference to file system." 
    fi = open("saved_references/%s_routeChange.pickle" % (expId), "w")
    pickle.dump(refRoutes, fi, 2) 
    
    sys.stderr.write("\n")
    pool.close()
    pool.join()
    
#TODO
def updateMagnitude(df=None, plotAsnData=False, metric="resp", 
        tau=5, tfidf_minScore=0.5, historySize=7*24, pltTitle="", exportCsv=False,
        minPeriods=3, asnList = []):

    expId = "583bb58ab0ab0284c6969c5d"
    if df is None:
        print "Retrieving Alarms"
        db = tools.connect_mongo()
        collection = db.routeChanges

        exp = db.routeExperiments.find_one({}, sort=[("$natural", -1)] )

        cursor = collection.find( {
                # "expId": exp["_id"], 
                "expId": objectid.ObjectId(expId),  # ipv4
                "corr": {"$lt": -0.2},
                "timeBin": {"$gt": datetime.datetime(2016,11,22, 0, 0, tzinfo=timezone("UTC")), "$lt": datetime.datetime(2016,11,23, 0, 0, tzinfo=timezone("UTC"))},
                # "timeBin": {"$gt": datetime.datetime(2015,11,1, 0, 0, tzinfo=timezone("UTC"))},
                "nbPeers": {"$gt": 2},
                # "nbSamples": {"$gt": 10},
            }, 
            [ 
                "obsNextHops",
                "refNextHops",
                "timeBin",
                "ip",
                "nbSamples",
                # "nbPeers",
                # "nbSeen",
                # "nbProbes":1,
                # "diff":1,
                "corr",
            # ], limit=300000
            ],
            # limit=200000
            )
        
        data = {"timeBin":[], "corrAbs": [], "router":[], "label": [], "ip": [], "pktDiffAbs": [],
                "pktDiff": [], "nbSamples": [],  "resp": [], "respAbs": [], "asn": []} # "nbPeers": [], "nbSeen": [],
        gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

        print "Compute stuff" # (%s rows)" % cursor.count() 
        for i, row in enumerate(cursor):
            print "%dk \r" % (i/1000), 
            obsList = row["obsNextHops"] #eval("{"+row["obsNextHops"].partition("{")[2][:-1])
            refDict = dict(row["refNextHops"]) #eval("{"+row["refNextHops"].partition("{")[2][:-1])
            sumPktDiff = 0
            for ip, pkt in obsList:
                sumPktDiff += np.abs(pkt - refDict[ip])

            for ip, pkt in obsList:
                if ip == "0":
                    continue

                pktDiff = pkt - refDict[ip] 
                if pktDiff < 0:
                    # link disapering
                    data["label"].append("-")
                else:
                    # new link
                    data["label"].append("+")

                pktDiffAbs = np.abs(pktDiff)
                corrAbs = np.abs(row["corr"])
                data["pktDiffAbs"].append(pktDiffAbs)
                data["pktDiff"].append(pktDiff)
                data["router"].append(row["ip"])
                data["timeBin"].append(row["timeBin"])
                data["corrAbs"].append(corrAbs)
                data["ip"].append(ip)
                data["nbSamples"].append(row["nbSamples"])
                data["respAbs"].append(corrAbs * (pktDiffAbs/sumPktDiff) )
                data["resp"].append(corrAbs * (pktDiff/sumPktDiff) )
                if ip == "x":
                    data["asn"].append("Pkt.Loss")
                else:
                    data["asn"].append(asn_by_addr(ip, db=gi)[0]) 
                # data["nbSeen"].append(row["nbSeen"])
                # data["nbPeers"].append(row["nbPeers"])
        
        print "\nRetrieve AS numbers"
        df =  pd.DataFrame.from_dict(data)
        df["timeBin"] = pd.to_datetime(df["timeBin"],utc=True)
        df.set_index("timeBin")

        print "Release memory...",
        del data
        gc.collect()
        print "done!"

        # fct = functools.partial(asn_by_addr, db=gi, onlyNumber=True)

        # find ASN for each ip
        # df["routerAsn"] = df["router"].apply(fct)
        # df["asn"] = df["ip"].apply(fct)
        # df.loc[df["ip"]=="x", "asn"] = "Pkt.loss"
        
        # df["asn"] = df["peerAsn"] #TODO add router ASN as well?
        # df["position"] = None
        # df.ix[df["routerAsn"]==df["peerAsn"], ["position"]] = "IN"
        # df.ix[df["routerAsn"]!=df["peerAsn"], ["position"]] = "OUT"
        # df["asn"] = df["peerAsn"] + df["label"] + df["position"] 

    if "prefix/24" not in df.columns:
        df["prefix/24"] = df["ip"].str.extract("([0-9]*\.[0-9]*\.[0-9]*)\.[0-9]*")+".0/24"

    # if "pktDiffAbs" not in df.columns:
        # df["pktDiffAbs"] = df["pktDiff"].abs()

    # if "corrAbs" not in df.columns:
        # df["corrAbs"] = df["corr"].abs()

    if "resp" not in df.columns:
        g = pd.DataFrame(df.groupby(["timeBin", "router"]).sum())
        df["resp"] = df["pktDiff"]/pd.merge(df, g,left_on=["timeBin","router"], 
                right_index=True, suffixes=["","_grp"])["pktDiffAbs_grp"] 
        df["resp"] = df["resp"]*df["corrAbs"]

    # Detect and plot events
    if tau > 0:
        secondAgg = "asn"
        groupAsn = df.groupby(["timeBin",secondAgg]).sum()
        groupAsn["asnL"] = groupAsn.index.get_level_values(secondAgg)
        groupAsn["timeBin"] = groupAsn.index.get_level_values("timeBin")
        groupAsn["asnLabel"] = "+"
        groupAsn.ix[groupAsn["resp"]<0, "asnLabel"] = "-"
        groupAsn[secondAgg] = groupAsn["asnLabel"]+groupAsn["asnL"]
        dfGrpAsn = pd.DataFrame(groupAsn)
        group = dfGrpAsn.groupby(level="timeBin").sum().resample("1H")
        ## Normalization 
        mad= lambda x: np.median(np.fabs(pd.notnull(x) -np.median(pd.notnull(x))))
        metricAbs = group[metric]
        group["metric"] = (metricAbs-pd.rolling_median(metricAbs,historySize,min_periods=minPeriods))/(1+1.4826*pd.rolling_apply(metricAbs,historySize,mad,min_periods=minPeriods))
        events = group[group["metric"].abs()> tau]

        fig = plt.figure(figsize=(9,3))
        ax = fig.add_subplot(111)
        ax.plot(group.index.get_level_values("timeBin"), group["metric"])
        ax.grid(True)
        plt.title(pltTitle)
        # plt.ylim([-2, 14])
        # plt.yscale("log")
        # plt.ylabel("Accumulated deviation")
        plt.ylabel("Magnitude (forwarding anomaly)")
        # plt.title(metric)
        
        print "Found %s events" % len(events)
        print events
    
        if tfidf_minScore > 0:
            tfidf(df, events, metric, historySize, tfidf_minScore, ax, group)
            
        fig.autofmt_xdate()
        plt.tight_layout()
        plt.savefig("fig/tfidf_route.eps")
        plt.close()

    if exportCsv:
        # asnFile = open("results/csv/forwarding_asn.csv","w")
        forwardingFile = open("results/csv/forwarding.csv","w")

    # Plot data per AS
    if plotAsnData or exportCsv:
        totalEvents = [] 
        for asn in dfGrpAsn["asnL"].unique():
            if asn!= "Pkt.Loss" and (len(asnList) == 0 or int(asn) in asnList):
                print asn
                fig = plt.figure(figsize=(10,4))
                # dfb = pd.DataFrame({u'resp':0.0, u'label':"", u'timeBin':datetime.datetime(2015,4,30,23), u'asn':asn,}, index=[datetime.datetime(2015,4,30,23)])
                # dfe = pd.DataFrame({u'resp':0.0, u'label':"", u'timeBin':datetime.datetime(2016,1,1,0), u'asn':asn}, index=[datetime.datetime(2016,1,1,0)])
                dfb = pd.DataFrame({u'resp':0.0, u'label':"", u'timeBin':datetime.datetime(2016,11,14,23), u'asn':asn,}, index=[datetime.datetime(2015,11,14,23)])
                dfe = pd.DataFrame({u'resp':0.0, u'label':"", u'timeBin':datetime.datetime(2016,11,26,0), u'asn':asn}, index=[datetime.datetime(2016,11,26,0)])
                dfasn = pd.concat([dfb, dfGrpAsn[dfGrpAsn["asnL"] == asn], dfe])
                grp = dfasn.groupby("timeBin")
                grpSum = grp.sum().resample("1H")
                grpCount = grp.count()

                
                mad= lambda x: np.median(np.fabs(pd.notnull(x) -np.median(pd.notnull(x))))
                grpSum["metric"] = (grpSum[metric]-pd.rolling_median(grpSum[metric],historySize,min_periods=minPeriods))/(1+1.4826*pd.rolling_apply(grpSum[metric],historySize,mad,min_periods=minPeriods))
                events = grpSum[grpSum["metric"]> tau]
                totalEvents.append(len(events))
                if len(events):
                    reportedAsn += 1

                if exportCsv:
                    # asnFile.write('%s,"%s"\n' % (asn, asname))
                    dftmp = pd.DataFrame(grpSum)
                    dftmp["asn"] = asn
                    dftmp["timeBin"] = dftmp.index
                    dftmp["label"] = "none" #TODO add tfidf results
                    dftmp.to_csv(forwardingFile, columns=["metric","resp", "label", "timeBin" ,"asn"],
                            header=False, index=False, date_format="%Y-%m-%d %H:%M:%S+00", na_rep="0")

                if plotAsnData:
                    try:
                        plt.plot(grpSum.index, grpSum["metric"])
                        plt.grid(True)
                        plt.title(asn)
                        plt.ylabel("Magnitude "+metric)
                        fig.autofmt_xdate()
                        plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s.eps" % asn))
                        plt.close()
                    except ValueError:
                        pass

                    # # Scatter plot magnitude vs. nb. ips
                    # fig = plt.figure()
                    # plt.plot(grpSum["metric"], grpCount["ip"], "*")
                    # plt.ylabel("# IPs")
                    # plt.xlabel("Magnitude")
                    # plt.grid(True)
                    # plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s_magnVSlink.eps" % asn))
                    # plt.close()

                    # Mean and Median
                    for u in ["corrAbs", "pktDiffAbs", "resp"]:
                        # Scatter plot magnitude vs. nb. links
                        grpMean = grp.mean()
                        grpMedian = grp.median()
                        fig = plt.figure(figsize=(10,4))
                        plt.plot(grpMean.index, grpMean[u], label="mean")
                        plt.plot(grpMedian.index, grpMedian[u], label="median")
                        plt.ylabel(u)
                        if u == "devBound":
                            plt.yscale("log")
                        plt.grid(True)
                        plt.title(asn)
                        fig.autofmt_xdate()
                        plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s_%s.eps" % (asn, u)))
                        plt.close()

                    # Sum
                    for u in ["corrAbs", "pktDiffAbs", "resp"]:
                        try:
                            # Scatter plot magnitude vs. nb. links
                            grpSum = grp.sum()
                            fig = plt.figure(figsize=(10,4))
                            plt.plot(grpSum.index, grpSum[u], label="sum")
                            plt.ylabel(u)
                            plt.yscale("log")
                            plt.grid(True)
                            plt.title(asn)
                            fig.autofmt_xdate()
                            plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s_%s_sum.eps" % (asn, u)))
                            plt.close()
                        except:
                            continue
                    # # std. dev.
                    # for u in ["corrAbs", "pktDiffAbs", "resp"]:
                        # grpStd = grp.std()
                        # fig = plt.figure(figsize=(10,4))
                        # plt.plot(grpStd.index, grpStd[u])
                        # plt.ylabel(u)
                        # # plt.yscale("log")
                        # plt.grid(True)
                        # plt.title(asn)
                        # # fig.autofmt_xdate()
                        # plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s_%s_std.eps" % (asn, u)))
                        # plt.close()

                    # # Number ips
                    # for u in ["corrAbs", "pktDiffAbs", "resp"]:
                        # if u == "resp":
                            # dfip = dfasn[dfasn[u]>0.9]
                        # else:
                            # dfip = dfasn[dfasn[u]>10]
                        # grpCrazyIP = dfip.groupby(["timeBin"])
                        # try:
                            # grpCrazy = grpCrazyIP.count()
                            # grpCount = grp.count()
                            # fig = plt.figure(figsize=(10,4))
                            # plt.plot(grpCount.index, grpCount["ip"], label="total")
                            # plt.plot(grpCrazy.index, grpCrazy["ip"], label="crazy")
                            # plt.ylabel("# reported IPs")
                            # # plt.yscale("log")
                            # plt.grid(True)
                            # plt.title(asn)
                            # fig.autofmt_xdate()
                            # plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s_%s_crazyIP.eps" % (asn, u)))
                            # plt.close()
                        # except ValueError:
                            # pass
                
    return df




def routeChangeDetection( (routes, routesRef, param, expId, ts, target) ):

    alpha = param["alpha"]
    alarms = []
    collection = db.routeChanges

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

    return (target, routesRef)


if __name__ == "__main__":
    expId = None
    if len(sys.argv)>1:
        if sys.argv[1] != "stream":
            expId = objectid.ObjectId(sys.argv[1]) 
        else:
            expId = "stream"
    detectRouteChangesMongo(expId)
