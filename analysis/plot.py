import statsmodels.api as sm
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pylab as plt
import tools
import rttAnalysis
import pandas as pd
from bson import objectid
import re
from pytz import timezone
import sys
import itertools
import datetime
from datetime import timedelta
from pytz import timezone
import calendar
import time
import os
import json
from bson import json_util
import glob
import numpy as np
from collections import defaultdict
from collections import deque
from scipy import stats
import pymongo
from multiprocessing import Process, JoinableQueue, Manager, Pool
import pygeoip
import functools
import socket
import cPickle as pickle
import gc

# from matplotlib import rc
# rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
# rc('text', usetex=True)

######## tool functions for plotting #########

def ecdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, yvals, **kwargs )



def prefixDiffRtt(res, prefix=None, minPts=20):
    
    nbAlarms = 0
    for k, v in res[0].iteritems():
        if not prefix is None and not k[0].startswith(prefix) and not k[1].startswith(prefix):
            continue
        if len(v)>minPts:
            nbAlarms += rttEvolution( (v, np.array(res[2][k])), k, prefix)

    print " %s alarms in total" % nbAlarms

def rttEvolution(res, ips, suffix):
    rttDiff = np.array(res)
    smoothAvg = []
    smoothHi = []
    smoothLow = []
    alarmsDates = []
    alarmsValues = []
    median = []
    ciLow = []
    ciHigh = []
    dates = []
    start = np.min(rttDiff[1])
    end = np.max(rttDiff[1])
    diff = end-start
    # dateRange = pd.date_range(start, end, freq="1H").tolist()
    # dateRange = range(start, end, 60*60)
    dateRange = [start+timedelta(hours=x) for x in range((diff.days+1)*24)] 

    for d in dateRange:
        # print d
        # print rttDiff[1]
        indices = rttDiff[1]==d
        dist = rttDiff[0][indices]
        if np.sum(indices) == 0: # or np.sum(dist) < 9:
            continue
        dates.append(d)
        median.append(np.median(dist))
        dist.sort()
        wilsonCi = sm.stats.proportion_confint(len(dist)/2, len(dist), 0.05, "wilson")
        wilsonCi = np.array(wilsonCi)*len(dist)
        ciLow.append( median[-1] - dist[int(wilsonCi[0])] )
        ciHigh.append( dist[int(wilsonCi[1])] - median[-1] )

        if not len(smoothAvg):
            smoothAvg.append(median[-1])
            smoothHi.append(dist[int(wilsonCi[1])])
            smoothLow.append(dist[int(wilsonCi[0])])
        else:
            smoothAvg.append(0.99*smoothAvg[-1]+0.01*median[-1])
            smoothHi.append(0.99*smoothHi[-1]+0.01*dist[int(wilsonCi[1])])
            smoothLow.append(0.99*smoothLow[-1]+0.01*dist[int(wilsonCi[0])])

            if median[-1]-ciLow[-1] > smoothHi[-1] or median[-1]+ciHigh[-1] < smoothLow[-1]: 
                alarmsDates.append(d)
                alarmsValues.append(median[-1])

    fig = plt.figure(figsize=(10,4))
    boundref = plt.fill_between(dates, smoothLow, smoothHi, color="0.5", facecolor="#DDDDFF", label="Normal Reference")
    # Workarround to have the fill_between in the legend
    boundref = plt.Rectangle((0, 0), 1, 1, fc="#DDDDFF", color="0.5")
    medianref, = plt.plot(dates, smoothAvg, '-', color="#AAAAFF")
    # plt.plot(dates, smoothHi, 'k--')
    # plt.plot(dates, smoothLow, 'k--')
    data = plt.errorbar(dates, median, [ciLow, ciHigh], fmt=".", ms=10, color="black", ecolor='0.25', label="Diff. RTT")
    ano, = plt.plot(alarmsDates, alarmsValues, "r*", ms=10, label="Anomaly")
    plt.grid(True, color="0.75")
    plt.title("%s - %s" % ips)
    fig.autofmt_xdate()
    plt.legend([data, (boundref, medianref), ano],["Measured Diff. RTT", "Normal Reference", "Detected Anomalies"], loc="best")
    plt.savefig("fig/diffRtt/%s_%s_%sAlarms_rttModel.eps" % (ips[0], ips[1], len(alarmsDates)))
    plt.close()

    fig = plt.figure()
    plt.plot(rttDiff[1], rttDiff[0],"x")
    plt.grid(True)
    # plt.yscale("log")
    plt.title("%s - %s" % ips)
    fig.autofmt_xdate() 
    plt.savefig("fig/diffRtt/%s_%s_%s.eps" % (suffix, ips[0], ips[1]))
    plt.close()

    return len(alarmsDates)

def getRttData():
    """

Notes: takes about 6G of RAM for 1 week of data for 1 measurement id
    """

    configFile = "conf/getRttData.conf"
    if os.path.exists(configFile):
        expParam = json.load(open(configFile,"r"), object_hook=json_util.object_hook)
    else:
        sys.stderr("No config file found!\nPlease copy conf/%s.default to conf/%s\n" % (configFile, configFile))

    pool = Pool(expParam["nbProcesses"],initializer=rttAnalysis.processInit) #, maxtasksperchild=binMult)

    # nbProcesses = 6
    # binMult = 3 
    # pool = Pool(nbProcesses,initializer=rttAnalysis.processInit) #, maxtasksperchild=binMult)

    # expParam = {
            # "timeWindow": 60*60, # in seconds 
            # "start": datetime.datetime(2015, 6, 1, 0, 0, tzinfo=timezone("UTC")), 
            # "end":   datetime.datetime(2015, 7, 1, 0, 0, tzinfo=timezone("UTC")),
            # "alpha": 0.01, 
            # "confInterval": 0.05,
            # "minASN": 3,
            # "minASNEntropy": 0.5,
            # "minSeen": 3,
            # "experimentDate": datetime.datetime.now(),
            # "af": "",
            # "comment": "Cogent and Level3 anomalies in June 2015",
            # "prefixes": "^154\.54|^130\.117\.14\.|^130\.117\.48|^4\.69|^67\.16\.133|^208\.178\.246"
            # }

    if not expParam["prefixes"] is None:
        expParam["prefixes"] = re.compile(expParam["prefixes"])
    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.rttExperiments
    alarmsCollection = db.rttChanges
    expId = detectionExperiments.insert_one(expParam).inserted_id 

    sampleMediandiff = {}
    ip2asn = {}
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    start = int(calendar.timegm(expParam["start"].timetuple()))
    end = int(calendar.timegm(expParam["end"].timetuple()))

    rawDiffRtt = defaultdict(list)
    rawNbProbes = defaultdict(list)
    rawDates = defaultdict(list) 

    for currDate in range(start,end,expParam["timeWindow"]):
        sys.stderr.write("Rtt analysis %s" % datetime.datetime.utcfromtimestamp(currDate))
        tsS = time.time()

        # Get distributions for the current time bin
        c = datetime.datetime.utcfromtimestamp(currDate)
        col = "traceroute%s_%s_%02d_%02d" % (expParam["af"], c.year, c.month, c.day) 
        # totalRows = db[col].count({ "timestamp": {"$gte": currDate, "$lt": currDate+expParam["timeWindow"]}, "result.result.from": expParam["prefixes"] })
        # if not totalRows:
            # print "No data for that time bin!"
            # continue
        params = []
        binEdges = np.linspace(currDate, currDate+expParam["timeWindow"], expParam["nbProcesses"]*expParam["binMult"]+1)
        for i in range(expParam["nbProcesses"]*expParam["binMult"]):
            params.append( (expParam["af"], binEdges[i], binEdges[i+1], 0, 0, expParam["prefixes"]) )

        diffRtt = defaultdict(dict)
        nbRow = 0 
        rttResults =  pool.imap_unordered(rttAnalysis.computeRtt, params)
        diffRtt, nbRow = rttAnalysis.mergeRttResults(rttResults, currDate, tsS, expParam["nbProcesses"]*expParam["binMult"])

        for k,v in diffRtt.iteritems():
            rawDiffRtt[k].extend(v["rtt"])
            # rawNbProbes[k].extend(v["probe"])
            rawDates[k].extend([c]*len(v["rtt"]))

        timeSpent = (time.time()-tsS)
        sys.stderr.write(", %s sec/bin,  %s row/sec\r" % (timeSpent, float(nbRow)/timeSpent))
    
    pool.close()
    pool.join()

    return (rawDiffRtt, rawNbProbes, rawDates)


def nbRttChanges(df=None, suffix="" ):

    if df is None:
        db = tools.connect_mongo()
        collection = db.rttChanges

        exp = db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        # for expId in expIds:
        cursor = collection.find({ 
                #"expId": exp["_id"], # DNS Root 60min time bin
                "expId": objectid.ObjectId("567f808ff7893768932b8334"), # probe diversity june 2015 
                }, 
            [
                "timeBin",
                "deviation",
                "diff",
                "ipPair",
                "timeBin",
                "nbSamples",
                "nbProbes",
            ])

        df =  pd.DataFrame(list(cursor))
        df["timeBin"] = pd.to_datetime(df["timeBin"],utc=True)
        df.set_index("timeBin")

    group = df[df["deviation"]>9].groupby("timeBin").sum()
    group["metric"] = group["deviation"]

    fig = plt.figure(figsize=(10,4))
    plt.plot(group.index, group["metric"]) 

    fig.autofmt_xdate()
    # plt.ylim([10**2, 10**6])
    plt.grid(True)
    plt.ylabel("Accumulated deviation")
    plt.yscale("log")
    plt.show()
       
    if suffix != "":
        suffix = "_"+suffix
    plt.savefig("fig/rttChanges_wilson%s.eps" % suffix)
    
    return df


def nbRouteChanges(expIds):

    db = tools.connect_mongo()
    collection = db.routeChanges

    targets = [
        "193.0.19.33",
        "192.33.4.12",
        "192.5.5.241",
        "199.7.83.42",
        "199.7.91.13",
        "128.63.2.53",
        "192.112.36.4",
        "198.41.0.4",
        "213.133.109.134",
        "192.228.79.201",
        "78.46.48.134",
        "192.58.128.30",
        "192.36.148.17",
        "193.0.14.129",
        "202.12.27.33",
        "192.203.230.10",
        "84.205.83.1"
]

    for dst_ip in targets:
        # for expId in expIds:
        cursor = collection.aggregate([
            # {"$match": {"dst_ip": dst_ip, "expId": objectid.ObjectId("566e8c41f789371f9038e813") }}, #, "corr": {"$lt": 0.05}}},
            # {"$match": {"dst_ip": dst_ip, "expId": objectid.ObjectId("5675143df7893746b3729fd0") }}, #, "corr": {"$lt": 0.05}}},
            {"$match": {"dst_ip": dst_ip, "expId": objectid.ObjectId("56775b72f789370348d58a02"),
            "corr": {"$lt": 0.05}}},
            # {"$match": {"nbSamples": {"$gt": 500}}},
            # {"$match": {"ip": {"$not": re.compile("probe.*")}}},
            # {"$match": {"expId": objectid.ObjectId(expId), 
                # "ip": {"$not": re.compile("probe.*")}}}, 
                # # "ipPair.0": {"$regex": re.compile("probe.*")}}}, 
            {"$group":{"_id": "$timeBin", "count": {"$sum":1}}},
            {"$sort": {"_id": 1}}
            ])

        # Expand the cursor and construct the DataFrame
        df =  pd.DataFrame(list(cursor))
        if not len(df):
            continue
        df["_id"] = pd.to_datetime(df["_id"],utc=True)
        df.set_index("_id")

        print "########## %s #############" % dst_ip
        event = df.ix[df["count"]> np.mean(df["count"])+3*np.std(df["count"])]["_id"]
        print event
    
        fig = plt.figure()
        # plt.plot_date(df["_id"],df["count"],tz=timezone("UTC"))
        plt.plot(df["_id"],df["count"]/3,label=dst_ip)
        # plt.plot(event, [0]*len(event), "r*")

        fig.autofmt_xdate()
        plt.grid(True)
        plt.ylabel("Nb. alarms")
        plt.yscale("log")
        plt.legend()
        plt.savefig("fig/routeChange_%s.eps" % dst_ip)
    

def sampleVsShapiro(results):

    for i, label in [(0,"Measured"), (1,"Inferred")]:
        if resutls[i] is None:
            continue

        plt.figure()

        shapiro = {}
        x = []
        y = []

        for k,v in results[i].iteritems():
            if len(v) > 40:
                x.append(np.mean(results[i+2][k]))
                y.append(stats.shapiro(v)[1])
        
        if i==0:
            plt.plot(x,y,'o')
            plt.title(label)

        plt.xscale("log")
        plt.yscale("log")
        plt.xlabel("Avg. # samples")
        plt.xlabel("Shapiro test")
        # plt.legend()
        plt.grid(True)
        plt.savefig("fig/nbSampleVsShapiro_%s.eps" % label)



def cLTMeanVsStd(results):
    
    # flatten the lists
    rng=None
    for i, label in [ (1, "Inferred"), (0, "Measured")]:
        if resutls[i] is None:
            continue

        data = results[i].values()
        #filtering
        data = filter(lambda x: len(x)>48, data)

        mean = map(np.mean,data) 
        std = map(np.std,data)

        # mean = np.random.choice(mean, (5000))
        # std = np.random.choice(std, (5000))

        plt.figure()
        #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        plt.plot(mean,std, "o") 
        plt.title("Distribution of the sample mean (%s)" % label)
        plt.grid(True)
        plt.xscale("log")
        plt.yscale("log")
        plt.xlabel("Mean")
        plt.ylabel("Std. dev.")
        xlim = plt.xlim()
        ylim = plt.ylim()
        minlim = np.max([np.min(xlim),np.min(ylim)])
        maxlim = np.min([np.max(xlim),np.max(ylim)])
        plt.plot([minlim, maxlim], [minlim, maxlim], "--k", lw=2)
        plt.savefig("fig/CLTMeanVsStd_%s.eps" % label)


def cLTMedianVsMad(results):
    
    # flatten the lists
    rng=None
    for i, label in [ (1, "Inferred"), (0, "Measured")]:
        if resutls[i] is None:
            continue

        data = results[i].values()

        #filtering
        data = filter(lambda x: len(x)>48, data)

        mean = map(np.median,data) 
        std = map(tools.mad,data)

        # mean = np.random.choice(mean, (5000))
        # std = np.random.choice(std, (5000))

        plt.figure()
        #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        plt.plot(mean,std, "o") 
        plt.title("Distribution of the sample mean (%s)" % label)
        plt.grid(True)
        plt.xscale("log")
        plt.yscale("log")
        plt.xlim([10**-4, 10**3])
        plt.ylim([10**-3, 10**3])
        plt.xlabel("Median")
        plt.ylabel("MAD")
        xlim = plt.xlim()
        ylim = plt.ylim()
        minlim = np.max([np.min(xlim),np.min(ylim)])
        maxlim = np.min([np.max(xlim),np.max(ylim)])
        plt.plot([minlim, maxlim], [minlim, maxlim], "--k")
        plt.savefig("fig/CLTMedianVsMad_%s.eps" % label)

# def distributionCLTStd(results):
    
    # # flatten the lists
    # measured = map(np.std,results[0].values()) 
    # inferred = map(np.std,results[1].values()) 
    # rng=None
    # for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        # plt.figure()
        # #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        # plt.hist(data, bins=50,  label=label, range=rng) 
        # plt.legend()
        # plt.grid(True)
        # plt.xscale("log")
        # plt.yscale("log")
        # plt.savefig("fig/distriutionCLTStd_%s.eps" % label)

# def distributionCLTMean(results):
    
    # # flatten the lists
    # measured = map(np.mean,results[0].values()) 
    # inferred = map(np.mean,results[1].values()) 
    # rng=None
    # for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        # plt.figure()
        # #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        # plt.hist(data, bins=50,  label=label, range=rng) 
        # plt.legend()
        # plt.grid(True)
        # plt.xscale("log")
        # plt.yscale("log")
        # plt.savefig("fig/distriutionCLTMean_%s.eps" % label)


def distributionNbSamples(results):
    
    # flatten the lists
    measured = list(itertools.chain.from_iterable(results[2].values()))
    inferred = list(itertools.chain.from_iterable(results[3].values()))
    rng = [0, 8000] 
    for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        plt.figure()
        plt.hist(data, bins=50,  label=label, range=rng)
        plt.legend()
        plt.grid(True)
        #plt.xscale("log")
        plt.yscale("log")
        plt.title("Mean = %s" % np.mean(data))
        plt.savefig("fig/distriutionNbSamples_%s.eps" % label)

def distributionAvgNbSamples(results):
    
    measured = filter(lambda x: len(x)>96, results[2].values())
    inferred  = filter(lambda x: len(x)>96, results[3].values())
    # flatten the lists
    measured = map(np.mean,measured)
    inferred = map(np.mean,inferred)
    rng = None #[3, 204] 
    for data, label in [(measured, "Measured"), (inferred, "Inferred")]:
        plt.figure()
        #rng = [np.min([np.min(measured),np.min(inferred)]), np.max([np.max(measured), np.max(inferred)])]
        plt.hist(data, bins=500,  label=label, range=rng)
        plt.legend()
        plt.grid(True)
        plt.xscale("log")
        plt.yscale("log")
        plt.title("Mean = %s" % np.mean(data))
        plt.savefig("fig/distriutionAvgNbSamples_%s.eps" % label)

def distributionShapiro(results):

    for data, label, samples in [(results[0], "Measured", results[2]), (results[1], "Inferred", results[3])]:
        plt.figure()
        data0 = []
        for k,v in data.iteritems():
            if len(v)>96:
                data0.append(stats.shapiro(v)[1])


        plt.hist(data0, bins=100, label=label)
        plt.legend()
        plt.grid(True)
        plt.yscale("log")
        plt.xscale("log")
        plt.xlabel("Shapiro p-value")
        plt.title("Mean = %0.3f" % np.mean(data0))
        plt.savefig("fig/distriutionShapiro_%s.eps" % label)


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


def country_by_addr(ip, db=None):
    try:
        return unicode(db.country_code_by_addr(ip)).encode("ascii", "ignore") #.partition(" ")[0]
    except socket.error:
        return "Unk"

def tfidf(df, events, metric, historySize, threshold, ax=None, group=None):
    nbDoc = df[metric].sum()
    alarms = []

    for bin in events.index:
        dfwin = df[(df["timeBin"] <= bin) & (df["timeBin"] >= bin-timedelta(hours=historySize))]
        print "Event: %s " % bin
        docLen = np.sum(dfwin[dfwin["timeBin"] == bin][metric])
        # plt.figure()
        x = []

        # maxVal = 0
        label = "" 

        voca = {}
        # voca["country"] = dfwin["country"].unique()
        if "asn" in dfwin.columns:
            voca["asn"] = dfwin["asn"].unique()
        if "prefix/24" in dfwin.columns: 
            voca["prefix/24"] = dfwin["prefix/24"].unique()
        for col, words in voca.iteritems():
            for word in words:
                nbDocWord = float(dfwin[dfwin[col] == word][metric].sum())
                wordFreq = float(dfwin[(dfwin["timeBin"] == bin) & (dfwin[col] == word)][metric].sum())

                tf = wordFreq/docLen
                tfidf = tf * np.log(1 + (nbDoc/nbDocWord))
                # tfidf = (asnFreq/docLen) * np.log(nbDoc/nbDocAsn)
                # tfidf = 1+np.log(asnFreq/docLen) * np.log(1+ (nbDoc/nbDocAsn))
                if tfidf > threshold:
                    print "\t%s, tfidf=%.2f, tf=%.2f" % (word, tfidf, tf)
                    # label += word.partition(" ")[0]+" (%d%%)\n" % (tf*100)
                    label += word+" (%d%%)\n" % (tf*100)

                if tf > 0.05:
                    print "\t%s, tfidf=%.2f, tf=%.2f" % (word, tfidf, tf)
                    
                # if tfidf > maxVal:
                    # maxVal = tfidf

                x.append(tfidf)

        # print "max asn: %s, %s occurences" % (maxLabel, maxVal)
        # plt.hist(x)
        # plt.savefig("tfidf_hist_%s.eps" % bin)
        alarms.append(label)
        if ax is not None:
            ax.annotate(str(bin)+":\n"+label, xy=(bin, group.ix[group.index==bin,"metric"]), xycoords='data',
                            xytext=(5, 0), textcoords='offset points',
                            horizontalalignment='left',
                            arrowprops=dict(arrowstyle="->"), size=8)

    return alarms



def routeEventCharacterization(df=None, plotAsnData=False, metric="resp", 
        tau=5, tfidf_minScore=0.5, historySize=7*24):

    if df is None:
        print "Retrieving Alarms"
        db = tools.connect_mongo()
        collection = db.routeChanges

        exp = db.routeExperiments.find_one({}, sort=[("$natural", -1)] )

        cursor = collection.find( {
                "expId": exp["_id"], 
                # "expId": objectid.ObjectId("5680df61f789371d76d2f0d6"), 
                "corr": {"$lt": -0.5},
                # "timeBin": {"$lt": datetime.datetime(2015,6,20, 0, 0, tzinfo=timezone("UTC"))},
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
            # ], limit=300000)
            ],
            # limit=200000
            )
        
        data = {"timeBin":[], "corrAbs": [], "router":[], "label": [], "ip": [], "pktDiffAbs": [],
                "pktDiff": [], "nbSamples": [],  "resp": [], "respAbs": [], "asn": []} # "nbPeers": [], "nbSeen": [],
        gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

        print "Compute stuff" # (%s rows)" % cursor.count() 
        for i, row in enumerate(cursor):
            print "%dk \r" % (i/1000), 
            obsDict = eval("{"+row["obsNextHops"].partition("{")[2][:-1])
            refDict = eval("{"+row["refNextHops"].partition("{")[2][:-1])
            sumPktDiff = 0
            for ip, pkt in obsDict.iteritems():
                sumPktDiff += np.abs(pkt - refDict[ip])

            for ip, pkt in obsDict.iteritems():
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

    # if "prefix/24" not in df.columns:
        # df["prefix/24"] = df["ip"].str.extract("([0-9]*\.[0-9]*\.[0-9]*)\.[0-9]*")+".0/24"

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
        groupAsn[metric] = groupAsn[metric].abs()
        dfGrpAsn = pd.DataFrame(groupAsn)
        group = dfGrpAsn.groupby(level="timeBin").sum()
        ## Normalization 
        mad = lambda x: np.median(np.fabs(x -np.median(x)))
        metricAbs = group[metric]
        group["metric"] = (metricAbs-pd.rolling_median(metricAbs,historySize))/(1.4826*pd.rolling_apply(metricAbs,historySize,mad))
        events = group[group["metric"]> tau]

        fig = plt.figure(figsize=(10,4))
        ax = fig.add_subplot(111)
        ax.plot(group.index.get_level_values("timeBin"), group["metric"])
        ax.grid(True)
        # plt.ylim([-2, 14])
        # plt.yscale("log")
        # plt.ylabel("Accumulated deviation")
        plt.ylabel("magnitude (Cor.)")
        # plt.title(metric)
        
        print "Found %s events" % len(events)
        print events
    
        if tfidf_minScore > 0:
            tfidf(dfGrpAsn, events, metric, historySize, tfidf_minScore, ax, group)
            
        fig.autofmt_xdate()
        plt.savefig("fig/tfidf_route.eps")
        plt.close()

    # Plot data per AS
    if plotAsnData:
        for asn in dfGrpAsn["asn"].unique():
            fig = plt.figure(figsize=(10,4))
            dfasn = dfGrpAsn[dfGrpAsn["asn"] == asn]
            grp = dfasn.groupby("timeBin")
            grpSum = grp.sum()
            grpCount = grp.count()
            # if grpSum.index.nunique() < 24:
                # continue
            # grp["metric"] = grp[metric]
            # grpSum["metric"] = (grpSum[metric]-pd.rolling_mean(grpSum[metric],historySize))/(pd.rolling_std(grpSum[metric],historySize))
            mad= lambda x: np.median(np.fabs(x -np.median(x)))
            grpSum["metric"] = (grpSum[metric]-pd.rolling_median(grpSum[metric],historySize))/(1.4826*pd.rolling_apply(grpSum[metric],historySize,mad))
            
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
            # std. dev.
            for u in ["corrAbs", "pktDiffAbs", "resp"]:
                grpStd = grp.std()
                fig = plt.figure(figsize=(10,4))
                plt.plot(grpStd.index, grpStd[u])
                plt.ylabel(u)
                # plt.yscale("log")
                plt.grid(True)
                plt.title(asn)
                fig.autofmt_xdate()
                plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s_%s_std.eps" % (asn, u)))
                plt.close()

            # Number ips
            for u in ["corrAbs", "pktDiffAbs", "resp"]:
                if u == "resp":
                    dfip = dfasn[dfasn[u]>0.9]
                else:
                    dfip = dfasn[dfasn[u]>10]
                grpCrazyIP = dfip.groupby(["timeBin"])
                try:
                    grpCrazy = grpCrazyIP.count()
                    grpCount = grp.count()
                    fig = plt.figure(figsize=(10,4))
                    plt.plot(grpCount.index, grpCount["ip"], label="total")
                    plt.plot(grpCrazy.index, grpCrazy["ip"], label="crazy")
                    plt.ylabel("# reported IPs")
                    # plt.yscale("log")
                    plt.grid(True)
                    plt.title(asn)
                    fig.autofmt_xdate()
                    plt.savefig("fig/routeChange_asn/"+metric+"/"+tools.str2filename("%s_%s_crazyIP.eps" % (asn, u)))
                    plt.close()
                except ValueError:
                    pass
                
    return df



def rttEventCharacterization(df=None, ref=None, plotAsnData=False, tau=5, tfidf_minScore=0.7,
        metric="devBound", unit = "devBound", historySize=7*24, exportCsv=False):
    
    if ref is None:
        db = tools.connect_mongo()
        exp = {"_id": objectid.ObjectId("56ae94f1f789376c5fbd8bd8")} #db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        print "Looking at experiment: %s" % exp["_id"]

        savedRef = pickle.load(open("saved_references/%s_diffRTT.pickle" % exp["_id"]))

        ga = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
        gc = pygeoip.GeoIP("../lib/GeoIP.dat")

        countAsn = defaultdict(int)
        countCountry = defaultdict(int)
        for k in savedRef.iterkeys():
            countAsn[asn_by_addr(k[0],db=ga)[0]]+=1
            countAsn[asn_by_addr(k[1],db=ga)[0]]+=1

            countCountry[country_by_addr(k[0],db=gc)]+=1
            countCountry[country_by_addr(k[1],db=gc)]+=1

        ref = {}
        ref["asn"] = pd.DataFrame(data={"asn": countAsn.keys(), "count": countAsn.values()})
        ref["country"] = pd.DataFrame(data={"country": countCountry.keys(), "count": countCountry.values()})

    if df is None:
        print "Retrieving Alarms"
        db = tools.connect_mongo()
        collection = db.rttChanges

        # exp = db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        exp = {"_id": objectid.ObjectId("56ae94f1f789376c5fbd8bd8")} #db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        print "Looking at experiment: %s" % exp["_id"]

        cursor = collection.aggregate([
            {"$match": {
                # "expId": objectid.ObjectId("5693c2e0f789373763a0bdf7"), 
                #"expId": objectid.ObjectId("5693c2e0f789373763a0bdf7"), 
                "expId": exp["_id"], 
                "timeBin": {"$gt": datetime.datetime(2015,6,15)},
                # "nbProbeASN": {"$gt": 2},
                # "asnEntropy": {"$gt": 0.5},
                # "expId": objectid.ObjectId("567f808ff7893768932b8334"), # probe diversity June 2015
                # "expId": objectid.ObjectId("5680de2af789371baee2d573"), # probe diversity 
                # "nbProbes": {"$gt": 4},
                }}, 
            {"$project": {
                "ipPair":1,
                "timeBin":1,
                "nbProbes":1,
                "entropy":1,
                "nbSamples":1,
                "diffMed":1,
                "diff":1,
                "deviation": 1,
                "devBound": 1,
                }},
            {"$unwind": "$ipPair"},
            ])

        df =  pd.DataFrame(list(cursor))
        df["timeBin"] = pd.to_datetime(df["timeBin"],utc=True)
        df.set_index("timeBin")
        print "got the data"

    if "diffMedAbs" not in df.columns:
        df["diffMedAbs"] = df["diffMed"].abs()
    
    if "diffAbs" not in df.columns:
        df["diffAbs"] = df["diff"].abs()
    
    if unit+"_Pkt" not in df.columns:
            # pkt/link unit
            df[unit+"_Pkt"] = df[unit]*df["nbSamples"]

    if "asn" not in df.columns:
        print "find AS numbers"
        # find ASN for each ip
        ga = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
        fct = functools.partial(asn_by_addr, db=ga)
        sTmp = df["ipPair"].apply(fct).apply(pd.Series)
        df["asn_name"] = sTmp[0]+" "+sTmp[1]
        df["asn"] = sTmp[0]
        df["asname"] = sTmp[1]

    # if unit+"_Asn" not in df.columns:
        # # AS unit: normalize by AS nb of links
        # g = df.groupby(["timeBin","asn"]).count()
        # df[unit+"_Asn"] = df[unit]/pd.merge(df, ref["asn"], on=["asn"])["count"]

    # if "country" not in df.columns:
        # # find country for each ip
        # gc = pygeoip.GeoIP("../lib/GeoIP.dat")
        # fct = functools.partial(country_by_addr, db=gc)
        # df["country"] = df["ipPair"].apply(fct)

    # if unit+"_Country" not in df.columns:
        # # Country unit: normalize by country
        # g = df.groupby(["timeBin","country"]).count()
        # df[unit+"_Country"] = df[unit]/pd.merge(df, ref["country"], on=["country"])["count"]

    # if unit+"_Earth" not in df.columns:
        # # Country unit: normalize by country
        # g = df.groupby(["timeBin","country"]).count()
        # df[unit+"_Earth"] = df[unit]/ref["country"]["count"].sum()

    if "prefix/24" not in df.columns:
        print "find prefixes"
        df["prefix/24"] = df["ipPair"].str.extract("([0-9]*\.[0-9]*\.[0-9]*)\.[0-9]*")+".0/24"

    # if "prefix/16" not in df.columns:
        # df["prefix/16"] = df["ipPair"].str.extract("([0-9]*\.[0-9]*)\.[0-9]*\.[0-9]*")+".0.0/16"

    group = df.groupby("timeBin").sum()
    ## Normalization 
    # count = df.groupby("timeBin").count()
    # group["metric"] = group[metric]/count["ipPair"]
    mad= lambda x: np.median(np.fabs(x -np.median(x)))
    group["metric"] = (group[metric]-pd.rolling_median(group[metric],historySize))/(1.4826*pd.rolling_apply(group[metric],historySize,mad))
    # group["metric"] = (group[metric]-pd.rolling_mean(group[metric],historySize))/(pd.rolling_std(group[metric],historySize))
    events = group[group["metric"]> tau]

    fig = plt.figure(figsize=(10,4))
    ax = fig.add_subplot(111)
    ax.plot(group.index, group["metric"])
    ax.grid(True)
    # plt.ylim([-2, 14])
    # plt.yscale("log")
    # plt.ylabel("Accumulated deviation")
    plt.ylabel("magnitude")
    # plt.title(metric)
    
    print "Found %s events" % len(events)
    print events

    if tfidf_minScore > 0:
        alarms = tfidf(df, events, metric, historySize, tfidf_minScore, ax, group)
    
    fig.autofmt_xdate()
    plt.savefig("fig/tfidf_%s.eps" % metric)

    if exportCsv:
        asnFile = open("results/csv/congestion_asn.csv","w")
        congestionFile = open("results/csv/congestion.csv","w")

    if plotAsnData:
        for asn_name in df["asn_name"].unique():

            asn = asn_name[0]
            fig = plt.figure(figsize=(10,4))
            dfasn = df[df["asn"] == asn]
            grp = dfasn.groupby("timeBin")
            grpSum = grp.sum()
            grpCount = grp.count()

            if len(grpSum)<240:
                continue

            # grp["metric"] = grp[metric]
            # grpSum["metric"] = (grpSum[metric]-pd.rolling_mean(grpSum[metric],historySize))/(pd.rolling_std(grpSum[metric],historySize))
            mad= lambda x: np.median(np.fabs(x -np.median(x)))
            grpSum["metric"] = (grpSum[metric]-pd.rolling_median(grpSum[metric],historySize))/(1.4826*pd.rolling_apply(grpSum[metric],historySize,mad))

            if exportCsv:
                asnFile.write("%s,%s\n" % asn_name)
                dftmp = pd.DataFrame(grpSum)
                dftmp["asn"] = asn
                dftmp["timeBin"] = dftmp.index
                dftmp["label"] = "" #TODO add tfidf results
                dftmp.to_csv(congestionFile, columns=["timeBin","asn","metric", devBound, "label"],
                        header=["timeBin", "asn", "magnitude", "absoluteDeviation", "label"])

            if plotAsnData:
                try:
                    plt.plot(grpSum.index, grpSum["metric"])
                    plt.grid(True)
                    plt.title(asn)
                    plt.ylabel("Magnitude "+metric)
                    fig.autofmt_xdate()
                    plt.savefig("fig/rttChange_asn/"+unit+"/"+tools.str2filename("%s.eps" % asn))
                    plt.close()
                except ValueError:
                    pass 

                # Scatter plot magnitude vs. nb. ips
                fig = plt.figure()
                plt.plot(grpSum["metric"], grpCount["ipPair"], "*")
                plt.ylabel("# IPs")
                plt.xlabel("Magnitude")
                plt.grid(True)
                plt.savefig("fig/rttChange_asn/"+unit+"/"+tools.str2filename("%s_magnVSlink.eps" % asn))
                plt.close()

                # Mean and Median
                for u in ["devBound", "diffAbs"]:
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
                    plt.savefig("fig/rttChange_asn/"+unit+"/"+tools.str2filename("%s_%s.eps" % (asn, u)))
                    plt.close()

                # Sum
                for u in ["devBound", "diffAbs"]:
                    # Scatter plot magnitude vs. nb. links
                    grpSum = grp.sum()
                    fig = plt.figure(figsize=(10,4))
                    plt.plot(grpSum.index, grpSum[u], label="sum")
                    plt.ylabel(u)
                    plt.yscale("log")
                    plt.grid(True)
                    plt.title(asn)
                    fig.autofmt_xdate()
                    plt.savefig("fig/rttChange_asn/"+unit+"/"+tools.str2filename("%s_%s_sum.eps" % (asn, u)))
                    plt.close()
                # std. dev.
                for u in ["devBound", "diffAbs"]:
                    grpStd = grp.std()
                    fig = plt.figure(figsize=(10,4))
                    plt.plot(grpStd.index, grpStd[u])
                    plt.ylabel(u)
                    # plt.yscale("log")
                    plt.grid(True)
                    plt.title(asn)
                    fig.autofmt_xdate()
                    plt.savefig("fig/rttChange_asn/"+unit+"/"+tools.str2filename("%s_%s_std.eps" % (asn, u)))
                    plt.close()

                # Number ips
                for u in ["devBound", "diffAbs"]:
                    dfip = dfasn[dfasn[u]>10]
                    grpCrazyIP = dfip.groupby(["timeBin"])
                    grpCrazy = grpCrazyIP.count()
                    grpCount = grp.count()
                    try:
                        fig = plt.figure(figsize=(10,4))
                        plt.plot(grpCount.index, grpCount["ipPair"], label="total")
                        plt.plot(grpCrazy.index, grpCrazy["ipPair"], label="crazy")
                        plt.ylabel("# reported IPs")
                        # plt.yscale("log")
                        plt.grid(True)
                        plt.title(asn)
                        fig.autofmt_xdate()
                        plt.savefig("fig/rttChange_asn/"+unit+"/"+tools.str2filename("%s_%s_crazyIP.eps" % (asn, u)))
                        plt.close()
                    except ValueError:
                        pass 

    if exportCsv:
        asnFile.close() 
        congestionFile.close() 

    return df, ref

def asRanking(df, ref, minNbIp=20, unit="diffAbs"):

    grpsum = pd.DataFrame(df.groupby("asn").sum()[unit])
    merged = pd.merge(grpsum, ref["asn"], left_index=True, right_on=["asn"])
    merged["score"] = merged[unit]/merged["count"]  

    rank = merged[merged["count"]>minNbIp].sort_values("score", ascending=False)
    rank /= df.timeBin.nunique()
    
    fi = open("results/rttChange_asnRanking_%s_min%sip.txt" % (unit, minNbIp),"w")
    fi.write(rank[["asn","score","count"]].to_string(index=False))

    return rank



def rttValidation(df, unit="devBound"):
    """ Print RTT diff. reference properties 

    """
    for y in ["nbProbes"]:
        plt.figure()
        plt.plot(df[unit], df[y],"*")
        plt.grid(True)
        plt.xlabel(unit)
        plt.ylabel(y)
        plt.yscale("log")
        plt.xscale("log")
        plt.title("r= %0.03f" % np.corrcoef(df[unit], df[y])[0,1])
        plt.savefig("./fig/rttChange_validation/%s_vs_%s.png" % (unit, y))
    



def rttRefStats(ref=None):
    """ Print RTT diff. reference properties 

    """
    if ref is None:
        # ref = pickle.load(open("./saved_references/567f808ff7893768932b8334_inferred.pickle"))
        # ref = pickle.load(open("./saved_references/5680de2af789371baee2d573_inferred.pickle"))
        # ref = pickle.load(open("./saved_references/5690b974f789370712b97cb4_inferred.pickle"))
        # ref = pickle.load(open("./saved_references/5693c2e0f789373763a0bdf7_inferred.pickle"))
        ref = pickle.load(open("saved_references/56ae94f1f789376c5fbd8bd8_diffRTT.pickle"))

    print "%s ip pairs" % len(ref)
 
    #### confidence intervals
    confUp = map(lambda x: x["high"] - x["mean"], ref.itervalues())
    confDown = map(lambda x: x["mean"] - x["low"], ref.itervalues())
    confSize = map(lambda x: x["high"] - x["low"], ref.itervalues())

    print "upper confidence interval size: %s (+- %s)" % (np.mean(confUp), np.std(confUp))
    print "\t min=%s max=%s" % (np.min(confUp), np.max(confUp))
    print "lower confidence interval size: %s (+- %s)" % (np.mean(confDown), np.std(confDown))
    print "\t min=%s max=%s" % (np.min(confDown), np.max(confDown))
    print "confidence interval size: %s (+- %s)" % (np.mean(confSize), np.std(confSize))
    print "confidence interval size: %s (+- %s) median" % (np.median(confSize), tools.mad(confSize))
    print "\t min=%s max=%s" % (np.min(confSize), np.max(confSize))


    plt.figure(figsize=(4,3))
    # plt.hist(confSize,bins=50, log=True)
    ecdf(confSize, label="interval size")
    ecdf(confDown, label="lower bound")
    ecdf(confUp, label="upper bound")
    plt.ylabel("CDF")
    plt.xscale("log")
    # plt.yscale("log")
    plt.grid(True)
    plt.legend()
    plt.xlim([10**-4, 10**4])
    plt.xlabel("Confidence interval size")
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_confInterval.eps")

    ### Number of probes
    nbProbes = map(lambda x: len(x["probe"]), ref.itervalues())
    print "total number of probes: %s (+- %s)" % (np.mean(nbProbes), np.std(nbProbes))
    print "total number of probes: %s (+- %s) median" % (np.median(nbProbes), tools.mad(nbProbes))
    print "\t min=%s max=%s" % (np.min(nbProbes), np.max(nbProbes))

    plt.figure(figsize=(4,3))
    # plt.hist(nbProbes,bins=50, log=True)
    ecdf(nbProbes)
    plt.ylabel("CDF")
    plt.xscale("log")
    # plt.yscale("log")
    plt.grid(True)
    plt.xlabel("Number of probes")
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_nbProbes.eps")
    plt.close()

    # correlation between the number of probes and the conf. interval size?

    confSize = np.array(confSize)
    idx0 = (confSize < 30000000)
    nbProbes = np.array(nbProbes)
    idx1 = (nbProbes < 65000000000)
    print np.corrcoef(confSize[idx0 & idx1], nbProbes[idx0 & idx1])
    print stats.spearmanr(confSize[idx0 & idx1], nbProbes[idx0 & idx1])

    #### Number of times seen / reported
    nbSeen = np.array([x["nbSeen"] for x in ref.itervalues()])
    nbReported = np.array([x["nbReported"] for x in ref.itervalues()])
    obsPeriod = np.array([1+(x["lastSeen"]-x["firstSeen"]).total_seconds() / 3600 for x in ref.itervalues()])
    
    plt.figure(figsize=(4,3))
    ecdf(nbSeen)
    plt.ylabel("CDF")
    plt.grid(True)
    plt.xlabel("# observations per link")
    plt.xscale("log")
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_nbSeen.eps")
    plt.close()

    plt.figure(figsize=(4,3))
    ecdf(nbReported)
    plt.ylabel("CDF")
    plt.grid(True)
    plt.xscale("log")
    plt.xlabel("# reports per link")
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_nbReported.eps")
    plt.close()

    plt.figure(figsize=(4,3))
    ecdf(obsPeriod)
    plt.ylabel("CDF")
    plt.grid(True)
    plt.xscale("log")
    plt.xlabel("Observation period per link (in hours)")
    plt.ylabel("CDF")
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_obsPeriod.eps")
    plt.close()

    plt.figure(figsize=(4,3))
    for bound in [(0, 1*24, "< 1day"), (1*24, 7*24, "< 1week"), (7*24, 30*24, "< 1month")]:
        n = nbSeen[(obsPeriod>=bound[0]) & (obsPeriod<bound[1])]
        o = obsPeriod[(obsPeriod>=bound[0]) & (obsPeriod<bound[1])]
        print "%s : %s pairs, observation ratio per link: mean=%s, median=%s" %(bound, len(n), np.mean(n/o), np.median(n/o))
        ecdf(n/o, label=bound[2])
    plt.grid(True)
    plt.xlabel("Observation ratio per link")
    plt.ylabel("CDF")
    plt.xlim([-0.1, 1.1])
    # plt.legend()
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_obsRatio_breakdown.eps")
    plt.close()

    plt.figure(figsize=(4,3))
    ecdf(nbSeen/obsPeriod)
    plt.plot([1/16., 1/16.], [0, 1])
    plt.plot([2/16., 2/16.], [0, 1])
    plt.plot([4/16., 4/16.], [0, 1])
    plt.plot([8/16., 8/16.], [0, 1])
    plt.grid(True)
    plt.ylabel("CDF")
    plt.xlabel("Observation ratio per link")
    plt.xlim([-0.1, 1.1])
    # plt.legend()
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_obsRatio.eps")
    plt.close()

    fig  = plt.figure(figsize=(10,4))
    data = defaultdict(int)
    for v in ref.itervalues():
        data[v["firstSeen"]] += 1 
    plt.plot(data.keys(), data.values(), "+")
    plt.ylim([0, 500])
    plt.grid(True)
    fig.autofmt_xdate()
    plt.ylabel("Number of new links")
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_newLinks.eps")
    plt.close()

    fig  = plt.figure(figsize=(10,4))
    data = defaultdict(int)
    for v in ref.itervalues():
        data[v["lastSeen"]] += 1 
    plt.plot(data.keys(), data.values(), "+")
    plt.ylim([0, 500])
    plt.grid(True)
    fig.autofmt_xdate()
    plt.ylabel("Number of links last seen")
    plt.tight_layout()
    plt.savefig("fig/rttChange_ref_endLinks.eps")
    plt.close()
