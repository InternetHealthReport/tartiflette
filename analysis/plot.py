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
import networkx as nx

# from matplotlib import rc
# rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
# rc('text', usetex=True)

######## tool functions for plotting #########

def ecdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, yvals, **kwargs )

def eccdf(a, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    plt.plot( sorted, 1-yvals, **kwargs )



def prefixDiffRtt(res, prefix=None, minPts=20):
    
    nbAlarms = 0
    for k, v in res[0].iteritems():
        if not prefix is None and not k[0].startswith(prefix) and not k[1].startswith(prefix):
            continue
        if len(v)>minPts:
            nbAlarms += rttEvolution( (v, np.array(res[2][k])), k, prefix)

    print " %s alarms in total" % nbAlarms

def rttEvolution(res, ips, suffix):
    ipNames = {
            "193.0.14.129": "K-root",
            "80.81.192.154": "RIPE NCC @ DE-CIX",
            "61.213.160.62": "NTT, Tokyo",
            "72.52.92.14": "HE, Frankfurt",
            "74.208.6.124": "1&1, Kansas City",
            "212.191.229.90": "Poznan, PL",
            "188.93.16.77": "Selectel, St. Petersburg",
            "95.213.189.0": "Selectel, Moscow",
            "130.117.0.250": "Cogent, Zurich",
            "154.54.38.50": "Cogent, Munich",
            }

    rttDiff = np.array(res)
    smoothAvg = []
    smoothHi = []
    smoothLow = []
    alarmsDates = []
    alarmsValues = []
    median = []
    mean = []
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
        if len(dist) < 3:
            continue
        dates.append(d)
        median.append(np.median(dist))
        mean.append(np.mean(dist))
        dist.sort()
        wilsonCi = sm.stats.proportion_confint(len(dist)/2, len(dist), 0.05, "wilson")
        wilsonCi = np.array(wilsonCi)*len(dist)
        ciLow.append( median[-1] - dist[int(wilsonCi[0])] )
        ciHigh.append( dist[int(wilsonCi[1])] - median[-1] )

        if len(smoothAvg)<24:
            smoothAvg.append(median[-1])
            smoothHi.append(dist[int(wilsonCi[1])])
            smoothLow.append(dist[int(wilsonCi[0])])
        elif len(smoothAvg)==24:
            smoothAvg.append(np.median(smoothAvg))
            smoothHi.append(np.median(smoothHi))
            smoothLow.append(np.median(smoothLow))
            for i in range(24):
                smoothAvg[i] = smoothAvg[-1]
                smoothHi[i] = smoothHi[-1]
                smoothLow[i] = smoothLow[-1]
        else:
            smoothAvg.append(0.99*smoothAvg[-1]+0.01*median[-1])
            smoothHi.append(0.99*smoothHi[-1]+0.01*dist[int(wilsonCi[1])])
            smoothLow.append(0.99*smoothLow[-1]+0.01*dist[int(wilsonCi[0])])

            if (median[-1]-ciLow[-1] > smoothHi[-1] or median[-1]+ciHigh[-1] < smoothLow[-1]) and np.abs(median[-1]-smoothAvg[-1])>1: 
                alarmsDates.append(d)
                alarmsValues.append(median[-1])

    if len(median)<2:
        return 0

    fig = plt.figure(figsize=(10,3))
    boundref = plt.fill_between(dates, smoothLow, smoothHi, color="#3333cc", facecolor="#DDDDFF", label="Normal Reference")
    # Workarround to have the fill_between in the legend
    boundref = plt.Rectangle((0, 0), 1, 1, fc="#DDDDFF", color="#3333cc")
    medianref, = plt.plot(dates, smoothAvg, '-', color="#AAAAFF")
    # plt.plot(dates, smoothHi, 'k--')
    # plt.plot(dates, smoothLow, 'k--')
    data = plt.errorbar(dates, median, [ciLow, ciHigh], fmt=".", ms=7, color="black", ecolor='0.33',  elinewidth=1, label="Diff. RTT")
    ano, = plt.plot(alarmsDates, alarmsValues, "r*", ms=10, label="Anomaly")
    plt.grid(True, color="0.75")
    # if ips[0] in ipNames and ips[1] in ipNames:
        # plt.title("%s (%s) - %s (%s)" % (ips[0],ipNames[ips[0]],ips[1],ipNames[ips[1]]))
    # else:
        # plt.title("%s - %s" % ips)
    fig.autofmt_xdate()
    if len(alarmsValues):
        plt.legend([data, (boundref, medianref), ano],["Median Diff. RTT", "Normal Reference", "Detected Anomalies"], loc="best")
    else:
        plt.legend([data, (boundref, medianref)],["Median Diff. RTT", "Normal Reference"], loc="best")
    plt.ylabel("Differential RTT (ms)")
    # plt.yscale("log")
    plt.tight_layout()
    plt.savefig("fig/diffRtt/%s_%s_%sAlarms_rttModel.eps" % (ips[0].replace(".","_"), ips[1].replace(".","_"), len(alarmsDates)))
    plt.close()

    f, ax = plt.subplots(1,1,figsize=(3.2,2.4))
    plt.ylim([-4, 4])
    plt.xlim([-4, 4])
    sm.qqplot(np.array(median), line="45", fit=True, ax=ax)
    # plt.title("Median")
    plt.grid(True, color="0.75")
    plt.ylabel("Median diff. RTT quantiles")
    plt.xlabel("Normal theoretical quantiles")
    plt.ylim([-4, 4])
    plt.xlim([-4, 4])
    plt.tight_layout()
    plt.savefig("fig/diffRtt/%s_%s_%sAlarms_qqplot_median.eps" % (ips[0].replace(".","_"), ips[1].replace(".","_"), len(alarmsDates)))
    plt.close()

    f, ax = plt.subplots(1,1,figsize=(3.2,2.4))
    sm.qqplot(np.array(mean), line="45", fit=True, ax=ax)
    # plt.title("Mean")
    plt.grid(True, color="0.75")
    plt.ylabel("Mean diff. RTT quantiles")
    plt.xlabel("Normal theoretical quantiles")
    plt.tight_layout()
    plt.savefig("fig/diffRtt/%s_%s_%sAlarms_qqplot_mean.eps" % (ips[0].replace(".","_"), ips[1].replace(".","_"), len(alarmsDates)))
    plt.close()


    plt.figure()
    plt.hist(rttDiff[0], bins=150)
    plt.grid(True, color="0.75")
    plt.yscale("log")
    plt.savefig("fig/diffRtt/%s_%s_distributionSamples.eps" % (ips[0].replace(".","_"), ips[1].replace(".","_")))
    plt.close()

    # fig = plt.figure(figsize=(10,4))
    fig = plt.figure(figsize=(20,2.5))
    plt.plot(rttDiff[1], rttDiff[0],"o",label="Raw values",ms=1)
    plt.grid(True)
    # plt.yscale("log")
    dat = np.array(rttDiff[0])
    m = np.mean(dat)
    s = np.std(dat)
    o = np.sum((dat>m+3*s) | (dat<m-3*s))
    operc = o/float(len(dat))

    # plt.title("%s - %s (mean=%.02f, std=%.02f, outliers=%s %.02f%%)" % (ips[0], ips[1], m, s, o, operc))
    plt.ylabel("Differential RTT (ms)") 
    if ips[0] in ipNames and ips[1] in ipNames:
        plt.title("%s (%s) - %s (%s)" % (ips[0],ipNames[ips[0]],ips[1],ipNames[ips[1]]))
    else:
        plt.title("%s - %s" % ips)
    plt.legend( loc="best")
    plt.ylim([100,300])
    plt.xticks([])
    fig.autofmt_xdate() 
    fig.tight_layout()
    plt.savefig("fig/diffRtt/%s_%s_samples.eps" % (ips[0].replace(".","_"), ips[1].replace(".","_")))
    plt.close()

    return len(alarmsDates)

def getRttData(configFile=None):
    """

Notes: takes about 6G of RAM for 1 week of data for 1 measurement id
    """

    if configFile is None:
        configFile = "conf/getRttData.conf"

    if os.path.exists(configFile):
        expParam = json.load(open(configFile,"r"), object_hook=json_util.object_hook)
    else:
        sys.stderr("No config file found!\nPlease copy conf/%s.default to conf/%s\n" % (configFile, configFile))

    pool = Pool(expParam["nbProcesses"],initializer=rttAnalysis.processInit) #, maxtasksperchild=binMult)

    if not expParam["prefixes"] is None:
        expParam["prefixes"] = re.compile(expParam["prefixes"])
    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas
    detectionExperiments = db.rttExperiments
    alarmsCollection = db.rttChanges
    # expId = detectionExperiments.insert_one(expParam).inserted_id 

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

def makeGraph(timeBin, af=4):
    db = tools.connect_mongo()
    collection = db.rttChanges

    if af==4:
        expId = "56d9b1cbb0ab021cc2102c10"
    else:
        expId = "56fe4405b0ab02f369ca8d2a" 

    cursor = collection.find({
        "expId": objectid.ObjectId(expId),
        # "timeBin": {"$in": timeBin},
        "timeBin": timeBin,
        # "$or": [{"diffMed":{"$lt": -1}}, {"diffMed":{"$gt": 1}}]
        "diffMed": {"$gt": 1} 
        })

    g = nx.Graph()
    for alarm in cursor:
        g.add_edge(alarm["ipPair"][0], alarm["ipPair"][1],{"dev":alarm["deviation"], "diffMed":alarm["diffMed"]})

    # cursor = collection.find({
        # "expId": objectid.ObjectId(""),
        # "timeBin": timeBin,
        # }) 
    #TODO add forwarding anomalies

    nx.write_gml(g, "graph/%s.gml" % timeBin)
    return g

def countASN(g):
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    count = defaultdict(int)
    for e in g.edges():
        s = set([asn_by_addr(e[0],gi), asn_by_addr(e[1],gi)])

        for asn in s:
            count[asn]+=1

    return count


def ref_countASN(ref):
    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

    count = defaultdict(int)
    for ip in ref.keys():
        s = set([asn_by_addr(ip[0],gi), asn_by_addr(ip[1],gi)])

        for asn in s:
            count[asn]+=1

    return count

def countRootServersDDoS():
    print "=========== IPv4 =============="
    gList = [makeGraph(datetime.datetime(2015,11,30, 7, 0, tzinfo=timezone("UTC")), 4),
            makeGraph(datetime.datetime(2015,11,30, 8, 0, tzinfo=timezone("UTC")), 4),
            makeGraph(datetime.datetime(2015,12,1, 5, 0, tzinfo=timezone("UTC")), 4)]
    ref4 = pickle.load(open("saved_references/56d9b1cbb0ab021cc2102c10_diffRTT.pickle","r"))

    res4 = countRootServers(ref4, gList, af=4)

    print "=========== IPv6 =============="
    gList = [makeGraph(datetime.datetime(2015,11,30, 7, 0, tzinfo=timezone("UTC")), 6),
            makeGraph(datetime.datetime(2015,11,30, 8, 0, tzinfo=timezone("UTC")), 6),
            makeGraph(datetime.datetime(2015,12,1, 5, 0, tzinfo=timezone("UTC")), 6)]
    ref6 = pickle.load(open("saved_references/56fe4405b0ab02f369ca8d2a_diffRTT.pickle","r"))

    res6 = countRootServers(ref4, gList, af=6)

    return (res4, res6)


def countRootServers(ref, gList, af=4):

    rootIpv4 = {"a.root-servers.net": "198.41.0.4", #2001:503:ba3e::2:30 VeriSign, Inc.
            "b.root-servers.net":  "192.228.79.201", #2001:500:84::b  University of Southern California (ISI)
            "c.root-servers.net":  "192.33.4.12", #2001:500:2::c  Cogent Communications
            "d.root-servers.net":  "199.7.91.13", #2001:500:2d::d University of Maryland
            "e.root-servers.net":  "192.203.230.10", #NASA (Ames Research Center)
            "f.root-servers.net":  "192.5.5.241", #2001:500:2f::f Internet Systems Consortium, Inc.
            "g.root-servers.net":  "192.112.36.4",#    US Department of Defense (NIC)
            "h.root-servers.net":  "198.97.190.53", #2001:500:1::53   US Army (Research Lab)
            "i.root-servers.net":  "192.36.148.17", #2001:7fe::53 Netnod
            "j.root-servers.net":  "192.58.128.30", #2001:503:c27::2:30   VeriSign, Inc.
            "k.root-servers.net":  "193.0.14.129", #2001:7fd::1   RIPE NCC
            "l.root-servers.net":  "199.7.83.42", #2001:500:9f::42    ICANN
            "m.root-servers.net":  "202.12.27.33", #2001:dc3::35  WIDE Project
            }
    rootIpv6 = {"a.root-servers.net": "2001:503:ba3e::2:30", # VeriSign, Inc.
            "b.root-servers.net":  "2001:500:84::b", #  University of Southern California (ISI)
            "c.root-servers.net":  "2001:500:2::c", #  Cogent Communications
            "d.root-servers.net":  "2001:500:2d::d", # University of Maryland
            # "e.root-servers.net":  "192.203.230.10", #NASA (Ames Research Center)
            "f.root-servers.net":  "2001:500:2f::f", # Internet Systems Consortium, Inc.
            # "g.root-servers.net":  "192.112.36.4",#    US Department of Defense (NIC)
            "h.root-servers.net":  "2001:500:1::53", #   US Army (Research Lab)
            "i.root-servers.net":  "2001:7fe::53", # Netnod
            "j.root-servers.net":  "2001:503:c27::2:30", #   VeriSign, Inc.
            "k.root-servers.net":  "2001:7fd::1", #   RIPE NCC
            "l.root-servers.net":  "2001:500:9f::42", #    ICANN
            "m.root-servers.net":  "2001:dc3::35", #  WIDE Project
            }

    if af == 4:
        rootIps = rootIpv4
    elif af == 6:
        rootIps = rootIpv6

    count = {}
    for k in rootIps.values():
        count[k] = {"all": set(), "reported": set()}

    for k in ref.keys():
        root = None
        if k[0] in rootIps.values():
            root = k[0]
        elif k[1] in rootIps.values():
            root = k[1]

        if not root is None:
            count[root]["all"].add(k)

    for g in gList:
        for k in g.edges_iter():
            root = None
            if k[0] in rootIps.values():
                root = k[0]
            elif k[1] in rootIps.values():
                root = k[1]

            if not root is None:
                count[root]["reported"].add(k)

    totalReported=0
    for k,v in count.iteritems():
        print "%s: %s out of %s reported pairs" % (k, len(v["reported"]), len(v["all"]))
        totalReported+=len(v["reported"])

    print "Total reported: %s" % totalReported

    return count



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

def count_nb_traceroutes():

    start = datetime.datetime(2015,5,1)
    end = datetime.datetime(2016,1,1)

    dateRange = [start+timedelta(i) for i in range((end-start).days)]
        
    db = tools.connect_mongo()

    total4 = 0
    total6 = 0
    for d in dateRange:
        
        colName = "traceroute%s_%s_%02d_%02d" % ("", d.year, d.month, d.day)
        col4 = db[colName]

        colName = "traceroute%s_%s_%02d_%02d" % ("6", d.year, d.month, d.day)
        col6 = db[colName]

        x4 = col4.count()
        x6 = col6.count()

        print "%s %d IPv4 %d IPv6" %(d,x4,x6)

        total4 += x4
        total6 += x6

    print "Total for v4 : %s" % total4
    print "Total for v6 : %s" % total6

def count_ref_ipLinks(ref):
    s = set()

    for ip in ref.keys():
        s.add(ip[0])
        s.add(ip[1])

    return s


def count_nb_routers(ref):
    s=set()    
    for routers in ref.values():
        s.update(routers.keys())

    probes = [ip for ip in s if ip.startswith("probe")] 
    routers = [ip for ip in s if not ip.startswith("probe")] 

    print "%s probes" % len(probes)
    print "%s routers" % len(routers)
    return (routers, probes) 


def count_ref_ipPerAsn(ref=None):

    gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
    
    if ref is None:
        ref = pickle.load(open("./saved_references/56d9b1eab0ab021d00224ca8_routeChange.pickle","r"))

    ipPerAsn = defaultdict(set)

    for targetRef in ref.values(): 
        for router, hops in targetRef.iteritems():
            for hop in hops.keys():
                if hop != "stats":
                    ipPerAsn[asn_by_addr(hop,gi)[0]].add(hop)

    return ipPerAsn

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
    nbDoc = np.abs(df[metric].sum())
    print "nbDoc = %s" % nbDoc
    alarms = []

    for bin in events.index:
        dfwin = df[(df["timeBin"] <= bin) & (df["timeBin"] >= bin-timedelta(hours=historySize))]
        print "Event: %s " % bin
        docLen = np.abs(np.sum(dfwin[dfwin["timeBin"] == bin][metric]))
        print "docLen = %s" % docLen
        # plt.figure()
        x = []

        # maxVal = 0
        label = "" 

        voca = {}
        # voca["country"] = dfwin["country"].unique()
        # if "asn" in dfwin.columns:
            # voca["asn"] = dfwin["asn"].unique()
        if "prefix/24" in dfwin.columns: 
            voca["prefix/24"] = dfwin["prefix/24"].unique()
        for col, words in voca.iteritems():
            for word in words:
                nbDocWord = np.abs(float(dfwin[dfwin[col] == word][metric].sum()))
                wordFreq = np.abs(float(dfwin[(dfwin["timeBin"] == bin) & (dfwin[col] == word)][metric].sum()))

                tf = wordFreq/docLen
                tfidf = tf * np.log(1 + (nbDoc/nbDocWord))
                # tfidf = (asnFreq/docLen) * np.log(nbDoc/nbDocAsn)
                # tfidf = 1+np.log(asnFreq/docLen) * np.log(1+ (nbDoc/nbDocAsn))
                if tfidf > threshold:
                    print "\t%s, tfidf=%.2f, tf=%.2f" % (word, tfidf, tf)
                    # label += word.partition(" ")[0]+" (%d%%)\n" % (tf*100)
                    label += word+" (%d%%)\n" % (tf*100)

                # if tf > 0.05:
                    # print "\t%s, tfidf=%.2f, tf=%.2f" % (word, tfidf, tf)
                    
                # if tfidf > maxVal:
                    # maxVal = tfidf

                x.append(tfidf)

        # print "max asn: %s, %s occurences" % (maxLabel, maxVal)
        # plt.hist(x)
        # plt.savefig("tfidf_hist_%s.eps" % bin)
        alarms.append(label)
        if ax is not None:
            pass
            # ax.annotate(str(bin)+":\n"+label, xy=(bin, group.ix[group.index==bin,"metric"]), xycoords='data',
                            # xytext=(5, 0), textcoords='offset points',
                            # horizontalalignment='left',
                            # arrowprops=dict(arrowstyle="->"), size=8)

    return alarms



def routeEventCharacterization(df=None, plotAsnData=False, metric="resp", 
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



def rttEventCharacterization(df=None, ref=None, plotAsnData=False, tau=5, tfidf_minScore=0.7,
        metric="devBound", unit = "devBound", historySize=7*24, exportCsv=False, pltTitle="",
        showDetection=True, minPeriods=1):

    expId = "583aca23b0ab020545bc2234" # Extra Emile 8.8.8.8
        # exp = {"_id": objectid.ObjectId("56d9b1cbb0ab021cc2102c10")} # IPv4 
        # exp = {"_id": objectid.ObjectId("56fe4405b0ab02f369ca8d2a")} # IPv6 
    
    if ref is None:
        db = tools.connect_mongo()
        exp = {"_id": objectid.ObjectId(expId)} 
        #db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        print "Looking at experiment: %s" % exp["_id"]

        savedRef = pickle.load(open("saved_references/%s_diffRTT.pickle" % exp["_id"]))

        # ga = pygeoip.GeoIP("../lib/GeoIPASNumv6.dat")
        # gc = pygeoip.GeoIP("../lib/GeoIPv6.dat")
        ga = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
        gc = pygeoip.GeoIP("../lib/GeoIP.dat")

        countAsn = defaultdict(int)
        countCountry = defaultdict(int)
        for k in savedRef.iterkeys():
            countAsn[asn_by_addr(k[0],db=ga)]+=1
            countAsn[asn_by_addr(k[1],db=ga)]+=1

            countCountry[country_by_addr(k[0],db=gc)]+=1
            countCountry[country_by_addr(k[1],db=gc)]+=1

        ref = {}
        ref["asn"] = pd.DataFrame(data={"asn": countAsn.keys(), "count": countAsn.values()})
        # return ref
        ref["country"] = pd.DataFrame(data={"country": countCountry.keys(), "count": countCountry.values()})
    
    if df is None:
        print "Retrieving Alarms"
        db = tools.connect_mongo()
        collection = db.rttChanges

        # exp = db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        exp = {"_id": objectid.ObjectId(expId)} #db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        print "Looking at experiment: %s" % exp["_id"]

        cursor = collection.aggregate([
            {"$match": {
                # "expId": objectid.ObjectId("5693c2e0f789373763a0bdf7"), 
                #"expId": objectid.ObjectId("5693c2e0f789373763a0bdf7"), 
                "expId": exp["_id"], 
                "timeBin": {"$gt": datetime.datetime(2016,11,15, 0, 0, tzinfo=timezone("UTC"))}, #, "$lt": datetime.datetime(2015,7,1, 0, 0, tzinfo=timezone("UTC"))},
                "diffMed": {"$gt": 1},
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
        # ga = pygeoip.GeoIP("../lib/GeoIPASNumv6.dat")
        ga = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
        fct = functools.partial(asn_by_addr, db=ga)
        sTmp = df["ipPair"].apply(fct).apply(pd.Series)
        df["asn_name"] = zip(sTmp[0], sTmp[1])
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
    mad= lambda x: np.median(np.fabs(pd.notnull(x) -np.median(pd.notnull(x))))
    group["metric"] = (group[metric]-pd.rolling_median(group[metric],historySize,min_periods=minPeriods))/(1+1.4826*pd.rolling_apply(group[metric],historySize,mad,min_periods=minPeriods))
    # group["metric"] = (group[metric]-pd.rolling_mean(group[metric],historySize))/(pd.rolling_std(group[metric],historySize))
    events = group[group["metric"]> tau]

    fig = plt.figure(figsize=(9,3))
    ax = fig.add_subplot(111)
    ax.plot(group.index, group["metric"])
    ax.grid(True)
    # plt.ylim([-2, 14])
    # plt.yscale("log")
    # plt.ylabel("Accumulated deviation")
    plt.ylabel("Magnitude (delay change)")
    plt.title(pltTitle)
    
    print "Found %s events" % len(events)
    print events

    if tfidf_minScore > 0:
        alarms = tfidf(df, events, metric, historySize, tfidf_minScore, ax, group)
    
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig("fig/tfidf_%s.eps" % metric)

    if exportCsv:
        asnFile = open("results/csv/congestion_asn.csv","w")
        congestionFile = open("results/csv/congestion.csv","w")

    if plotAsnData or exportCsv or showDetection:
        totalEvents = [] 
        reportedAsn = 0
        for asn, asname in ref["asn"]["asn"].unique(): #df["asn_name"].unique():

            dfb = pd.DataFrame({u'devBound':0.0, u'label':"", u'timeBin':datetime.datetime(2016,11,14,23), u'asn':asn,}, index=[datetime.datetime(2016,11,14,23)])
            dfe = pd.DataFrame({u'devBound':0.0, u'label':"", u'timeBin':datetime.datetime(2016,11,30,0), u'asn':asn}, index=[datetime.datetime(2016,11,30,0)])
            dfasn = pd.concat([dfb, df[df["asn"] == asn], dfe])

            grp = dfasn.groupby("timeBin")
            grpSum = grp.sum().resample("1H")
            grpCount = grp.count()


            # grp["metric"] = grp[metric]
            # grpSum["metric"] = (grpSum[metric]-pd.rolling_mean(grpSum[metric],historySize))/(pd.rolling_std(grpSum[metric],historySize))
            mad= lambda x: np.median(np.fabs(pd.notnull(x) -np.median(pd.notnull(x))))
            grpSum["metric"] = (grpSum[metric]-pd.rolling_median(grpSum[metric],historySize,min_periods=minPeriods))/(1+1.4826*pd.rolling_apply(grpSum[metric],historySize,mad,min_periods=minPeriods))
            events = grpSum[grpSum["metric"]> tau]
            totalEvents.append(len(events))
            if len(events):
                reportedAsn += 1
            else:
                continue

            if exportCsv:
                asnFile.write('%s,"%s"\n' % (asn, asname))
                dftmp = pd.DataFrame(grpSum)
                dftmp["asn"] = asn
                dftmp["timeBin"] = dftmp.index
                dftmp["label"] = "none" #TODO add tfidf results
                dftmp.to_csv(congestionFile, columns=["metric","devBound", "label", "timeBin" ,"asn"],
                        header=False, index=False, date_format="%Y-%m-%d %H:%M:%S+00", na_rep="0")

            if plotAsnData:
                try:
                    fig = plt.figure(figsize=(10,4))
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
                # fig = plt.figure()
                # plt.plot(grpSum["metric"], grpCount["ipPair"], "*")
                # plt.ylabel("# IPs")
                # plt.xlabel("Magnitude")
                # plt.grid(True)
                # plt.savefig("fig/rttChange_asn/"+unit+"/"+tools.str2filename("%s_magnVSlink.eps" % asn))
                # plt.close()

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

        print "Total nb. of detected events: mean=%s, median=%s, std=%s, sum=%s" % (np.mean(totalEvents), np.median(totalEvents), np.std(totalEvents), np.sum(totalEvents))
        print "Total reported ASs: %s/%s" % (reportedAsn,len(df["asn_name"].unique()))

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
    

def rttValidation2(ref):
    
    db = tools.connect_mongo()
    collection = db.rttChanges
    
    cursor = collection.aggregate([
             {
            "$match": {"expId": objectid.ObjectId("56d9b1cbb0ab021cc2102c10"), 
                 # "$or": [ {"diffMed": {"$gt": 1}}, {"diffMed": {"$lt": -1}}]
                 }},   
            { "$group": { "_id": "$nbProbes", "count": { "$sum": 1 }}  }
            ])

    nbAlarms = pd.DataFrame(list(cursor))

    count = defaultdict(list)
    for v in ref.itervalues():
        # if v["nbSeen"] >24:
            count[len(v["probe"])].append(v["nbReported"])

   
    mean = {k: float(nbAlarms[nbAlarms["_id"] == k]["count"].sum())/len(v) for k,v in count.iteritems()}

    return mean


def rttRefStats(ref=None):
    """ Print RTT diff. reference properties 

    """
    if ref is None:
        # ref = pickle.load(open("./saved_references/567f808ff7893768932b8334_inferred.pickle"))
        # ref = pickle.load(open("./saved_references/5680de2af789371baee2d573_inferred.pickle"))
        # ref = pickle.load(open("./saved_references/5690b974f789370712b97cb4_inferred.pickle"))
        # ref = pickle.load(open("./saved_references/5693c2e0f789373763a0bdf7_inferred.pickle"))
        ref = pickle.load(open("saved_references/56d9b1cbb0ab021cc2102c10_diffRTT.pickle"))


    print "%s ip pairs" % len(ref)
 
    #### confidence intervals
    confUp = map(lambda x: np.mean(x["high"]) - np.mean(x["mean"]), ref.itervalues())
    confDown = map(lambda x: np.mean(x["mean"]) - np.mean(x["low"]), ref.itervalues())
    confSize = map(lambda x: np.mean(x["high"]) - np.mean(x["low"]), ref.itervalues())
    nbSeen = map(lambda x: int(x["nbSeen"]), ref.itervalues())

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
    idx0 = (confSize < 10000000)
    nbProbes = np.array(nbProbes)
    idx1 = (nbProbes > 3)
    nbSeen = np.array(nbSeen)
    idx0 = (nbSeen > 24)
    print idx0
    print confSize[idx0 & idx1]
    print nbProbes[idx0 & idx1]
    print np.corrcoef(confSize[idx0 & idx1], nbProbes[idx0 & idx1])
    print stats.spearmanr(confSize[idx0 & idx1], nbProbes[idx0 & idx1])
    plt.figure(figsize=(4,3))
    plt.plot(nbProbes[idx0 & idx1], confSize[idx0 & idx1], ".", ms=2)
    plt.xlabel("# probes")
    plt.ylabel("Ref. size")
    plt.grid(True, color="0.5")
    plt.xscale("log")
    plt.yscale("log")
    plt.tight_layout()
    plt.savefig("fig/rttChange_nbProbes_vs_refSize.eps")

    #### Number of times seen / reported
    nbSeen = np.array([x["nbSeen"] for x in ref.itervalues()])
    nbReported = np.array([x["nbReported"] for x in ref.itervalues()])
    obsPeriod = np.array([1+(x["lastSeen"]-x["firstSeen"]).total_seconds() / 3600 for x in ref.itervalues()])
    nbReportedLinks = np.sum(nbReported != 0)
    nbNotReportedLinks = np.sum(nbReported == 0)
    print "%s (%s%%) reported links" % (nbReportedLinks, nbReportedLinks/float(len(ref)))
    print "%s (%s%%) not reported links" % (nbNotReportedLinks, nbNotReportedLinks/float(len(ref)))
    
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

    return ref

def routeRefStat(ref):

    nbHops = []
    nbReported = []
    for dest, data in ref.iteritems():

        for k, v in data.iteritems():
            if k == "x":
                continue
            if len(v) > 1000:
                print k
            nbHops.append(len(v)-2)

            if v["stats"]["nbReported"]:
                nbReported.append(1)
            else:
                nbReported.append(0)

    return (nbHops, nbReported)


def exportAlarmsCsv(rtt="./results/csv/rttAlarms.csv", route="./results/csv/routeAlarms.csv"):
    
    if not route is None:
        fi = open(route,"w")

        print "Retrieving forwarding alarms"
        db = tools.connect_mongo()
        collection = db.routeChanges

        exp = db.routeExperiments.find_one({}, sort=[("$natural", -1)] )

        cursor = collection.find( {
                # "expId": exp["_id"], 
                "expId": objectid.ObjectId("56d9b1eab0ab021d00224ca8"),  # ipv4
                "corr": {"$lt": -0.2},
                # "timeBin": {"$gt": datetime.datetime(2015,5,1, 0, 0, tzinfo=timezone("UTC")), "$lt": datetime.datetime(2015,6,1, 0, 0, tzinfo=timezone("UTC"))},
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
        
        gi = pygeoip.GeoIP("../lib/GeoIPASNum.dat")

        print "Compute stuff" # (%s rows)" % cursor.count() 
        for i, row in enumerate(cursor):
            data = {"timeBin":[], "corr": [], "router":[],  "ip": [], 
                    "pktDiff": [], "nbSamples": [],  "resp": []} 
            print "%dk \r" % (i/1000), 
            obsList = row["obsNextHops"] 
            refDict = dict(row["refNextHops"]) 
            sumPktDiff = 0
            for ip, pkt in obsList:
                sumPktDiff += np.abs(pkt - refDict[ip])

            for ip, pkt in obsList:
                if ip == "0":
                    continue

                pktDiff = pkt - refDict[ip] 
                pktDiffAbs = np.abs(pktDiff)
                corrAbs = np.abs(row["corr"])
                data["pktDiff"].append(pktDiff)
                data["router"].append(row["ip"])
                data["timeBin"].append(row["timeBin"])
                data["corr"].append(row["corr"])
                data["ip"].append(ip)
                data["nbSamples"].append(row["nbSamples"])
                data["resp"].append(corrAbs * (pktDiff/sumPktDiff) )

            maxInd = np.argmax(data["resp"])
            minInd = np.argmin(data["resp"])
            
            fi.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (
                data["timeBin"][maxInd], data["router"][maxInd],
                data["corr"][maxInd], data["nbSamples"][maxInd],
                data["ip"][minInd],asn_by_addr(data["ip"][minInd], db=gi)[0],
                data["resp"][minInd], data["pktDiff"][minInd],
                data["ip"][maxInd],asn_by_addr(data["ip"][maxInd], db=gi)[0],
                data["resp"][maxInd], data["pktDiff"][maxInd]
             ))
        fi.close() 

    if not rtt is None:
        fi = open(rtt,"w")
        print "Retrieving Delay Change Alarms"
        db = tools.connect_mongo()
        collection = db.rttChanges

        # exp = db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        exp = {"_id": objectid.ObjectId("56d9b1cbb0ab021cc2102c10")} #db.rttExperiments.find_one({}, sort=[("$natural", -1)] )
        print "Looking at experiment: %s" % exp["_id"]

        cursor = collection.aggregate([
            {"$match": {
                # "expId": objectid.ObjectId("5693c2e0f789373763a0bdf7"), 
                #"expId": objectid.ObjectId("5693c2e0f789373763a0bdf7"), 
                "expId": exp["_id"], 
                # "timeBin": {"$gt": datetime.datetime(2015,6,1, 0, 0, tzinfo=timezone("UTC")), "$lt": datetime.datetime(2015,7,1, 0, 0, tzinfo=timezone("UTC"))},
                "diffMed": {"$gt": 1},
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
                "median":1,
                "ref": 1,
                "devBound": 1,
                }},
            ])

        df =  pd.DataFrame(list(cursor))
        df["timeBin"] = pd.to_datetime(df["timeBin"],utc=True)
        df.set_index("timeBin")
        print "got the data"

        if "asn" not in df.columns:
            print "find AS numbers"
            # find ASN for each ip
            # ga = pygeoip.GeoIP("../lib/GeoIPASNumv6.dat")
            ga = pygeoip.GeoIP("../lib/GeoIPASNum.dat")
            fct = functools.partial(asn_by_addr, db=ga)
            ipPair = df["ipPair"].apply(pd.Series)
            df["ip0"] = ipPair[0]
            df["ip1"] = ipPair[1]
            df["asn0"] = df["ip0"].apply(fct).apply(pd.Series)[0]
            df["asn1"] = df["ip1"].apply(fct).apply(pd.Series)[0]

        
        df["label"] = "none" #TODO add tfidf results
        df.to_csv(fi, columns=["timeBin","asn0","asn1","ip0","ip1",
            "devBound","diffMed","ref","median","label"],
            header=False, index=False, date_format="%Y-%m-%d %H:%M:%S+00", na_rep="0")
        fi.close()


def magnitudeDistribution(congestion, forwarding):

    if congestion is None:
        congestion = pd.read_csv("results/csv/congestion.csv", header=None, names=["mag", "devBound", "label", "time", "asn"])

    if forwarding is None:
        forwarding = pd.read_csv("results/csv/forwarding.csv", header=None, names=["mag", "devBound", "label", "time", "asn"]) 

    # Plot the CDF for congestions
    plt.figure()
    eccdf(congestion.mag, marker="o")
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("Magnitude (delay change)")
    plt.xlim([1, 10e5])
    plt.ylabel("CCDF")
    plt.tight_layout()
    plt.savefig("./fig/magnitude_congestion_cdf.eps")


    # Plot the CDF for forwarding anomalies
    plt.figure()
    ecdf(forwarding.mag,marker="o")
    plt.xlabel("Magnitude (forwarding anomaly)")
    plt.ylabel("CDF")
    plt.yscale("log")
    plt.xlim([-50, 5])
    plt.tight_layout()
    plt.savefig("./fig/magnitude_forwarding_cdf.eps")


