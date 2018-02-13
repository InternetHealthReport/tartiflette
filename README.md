# tartiflette: Pinpointing delay and forwarding anomalies from large-scale traceroute measurements.

Our results with the RIPE Atlas traceroute data is available here: http://romain.iijlab.net/ihr/


## Requirements
- pandas
- matplotlib
- pymongo
- pygeoip
- statsmodels

You also need a mongodb server. (see below)
This product includes GeoLite data created by MaxMind, available from http://www.maxmind.com

## MongoDB
The code currently reads traceroute from a mongoDB server and store results in that same server.
For now the name of the mongodb server is hardcoded to "mongodb-
iijlab". So you can just create an alias (e.g. in /etc/hosts on linux) that points to your server.

### Configuration
First you need to create a database called: atlas
This database will hold traceroutes and tartiflette's results.

Traceroutes from the same day are collected in a single collection (i.e. the equivalent of a table in Mongo), and the name of the collections should follow the following template: traceroute_YYYY_MM_DD
For example traceroutes collected on May 15th, 2017, are stored in collection traceroute_2017_05_15.

Traceroutes should have the same format as the ones given by RIPE Atlas.
Here is an example of traceroutes stored in our Mongo database:
```
> use atlas
switched to db atlas
> db.traceroute_2017_05_15.find().limit(3)
{ "_id" : ObjectId("5918efa91cc609ca3e838a1d"), "lts" : 43, "size" : 40, "src_addr" : "10.128.0.1", "msm_id" : 5013, "proto" : "UDP", "timestamp" : 1494806404, "msm_name" : "Traceroute", "fw" : 4770, "paris_id" : 15, "prb_id" : 20069, "af" : 4, "result" : [ { "result" : [ { "rtt" : 8.164, "ttl" : 63, "from" : "10.29.234.1", "size" : 68 }, { "rtt" : 9.211, "ttl" : 63, "from" : "10.29.234.1", "size" : 68 }, { "rtt" : 16.346, "ttl" : 63, "from" : "10.29.234.1", "size" : 68 } ], "hop" : 1 }, { "result" : [ { "rtt" : 21.304, "ttl" : 250, "from" : "75.154.217.108", "size" : 28 }, { "rtt" : 20.696, "ttl" : 250, "from" : "75.154.217.108", "size" : 28 }, { "rtt" : 20.7, "ttl" : 250, "from" : "75.154.217.108", "size" : 28 } ], "hop" : 2 }, { "result" : [ { "rtt" : 17.903, "ttl" : 250, "from" : "206.81.80.80", "size" : 68 }, { "rtt" : 17.951, "ttl" : 250, "from" : "206.81.80.80", "size" : 68 }, { "rtt" : 18.488, "ttl" : 250, "from" : "206.81.80.80", "size" : 68 } ], "hop" : 3 }, { "result" : [ { "rtt" : 20.694, "ttl" : 60, "from" : "192.203.230.10", "size" : 68 }, { "rtt" : 20.575, "ttl" : 60, "from" : "192.203.230.10", "size" : 68 }, { "rtt" : 20.805, "ttl" : 60, "from" : "192.203.230.10", "size" : 68 } ], "hop" : 4 } ], "from" : "50.69.116.108", "endtime" : 1494806404, "type" : "traceroute", "dst_addr" : "192.203.230.10", "dst_name" : "192.203.230.10" }
{ "_id" : ObjectId("5918efad1cc609ca3e838aa3"), "lts" : 42, "size" : 40, "src_addr" : "192.168.0.156", "msm_id" : 5017, "proto" : "UDP", "timestamp" : 1494806404, "msm_name" : "Traceroute", "fw" : 4760, "paris_id" : 1, "prb_id" : 29612, "af" : 4, "result" : [ { "result" : [ { "rtt" : 1.105, "ttl" : 64, "from" : "192.168.0.1", "size" : 68 }, { "rtt" : 0.968, "ttl" : 64, "from" : "192.168.0.1", "size" : 68 }, { "rtt" : 0.911, "ttl" : 64, "from" : "192.168.0.1", "size" : 68 } ], "hop" : 1 }, { "result" : [ { "rtt" : 17.797, "ttl" : 63, "from" : "83.169.183.65", "size" : 68 }, { "rtt" : 15.791, "ttl" : 63, "from" : "83.169.183.65", "size" : 68 }, { "rtt" : 39.786, "ttl" : 253, "from" : "83.169.129.77", "size" : 68 } ], "hop" : 2 }, { "result" : [ { "rtt" : 15.109, "ttl" : 252, "from" : "88.134.235.186", "size" : 68 }, { "rtt" : 10.408, "ttl" : 252, "from" : "88.134.235.186", "size" : 68 }, { "rtt" : 13.63, "ttl" : 252, "from" : "88.134.235.186", "size" : 68 } ], "hop" : 3 }, { "result" : [ { "rtt" : 19.749, "ttl" : 251, "from" : "88.134.235.96", "size" : 68 }, { "rtt" : 19.749, "ttl" : 251, "from" : "88.134.235.96", "size" : 68 }, { "rtt" : 19.762, "ttl" : 251, "from" : "88.134.235.96", "size" : 68 } ], "hop" : 4 }, { "result" : [ { "rtt" : 19.696, "ttl" : 250, "from" : "88.134.234.205", "size" : 68 }, { "rtt" : 19.717, "ttl" : 250, "from" : "88.134.234.205", "size" : 68 }, { "rtt" : 19.784, "ttl" : 250, "from" : "88.134.234.205", "size" : 68 } ], "hop" : 5 }, { "result" : [ { "rtt" : 19.796, "ttl" : 249, "from" : "213.133.113.237", "size" : 28 }, { "rtt" : 24.53, "ttl" : 249, "from" : "213.133.113.237", "size" : 28 }, { "rtt" : 19.73, "ttl" : 249, "from" : "213.133.113.237", "size" : 28 } ], "hop" : 6 }, { "result" : [ { "rtt" : 23.734, "ttl" : 248, "from" : "213.239.245.9", "size" : 28 }, { "rtt" : 20.711, "ttl" : 248, "from" : "213.239.245.9", "size" : 28 }, { "rtt" : 19.906, "ttl" : 248, "from" : "213.239.245.9", "size" : 28 } ], "hop" : 7 }, { "result" : [ { "rtt" : 24.606, "ttl" : 247, "from" : "213.239.245.178", "size" : 28 }, { "rtt" : 24.866, "ttl" : 247, "from" : "213.239.245.178", "size" : 28 }, { "rtt" : 26.423, "ttl" : 247, "from" : "213.239.245.178", "size" : 28 } ], "hop" : 8 }, { "result" : [ { "rtt" : 24.209, "ttl" : 246, "from" : "213.239.229.70", "size" : 28 }, { "rtt" : 25.075, "ttl" : 246, "from" : "213.239.229.70", "size" : 28 }, { "rtt" : 24.025, "ttl" : 246, "from" : "213.239.229.70", "size" : 28 } ], "hop" : 9 }, { "result" : [ { "rtt" : 29.126, "ttl" : 54, "from" : "78.46.48.134", "size" : 68 }, { "rtt" : 29.65, "ttl" : 54, "from" : "78.46.48.134", "size" : 68 }, { "rtt" : 29.731, "ttl" : 54, "from" : "78.46.48.134", "size" : 68 } ], "hop" : 10 } ], "from" : "95.91.211.162", "endtime" : 1494806405, "type" : "traceroute", "dst_addr" : "78.46.48.134", "dst_name" : "78.46.48.134" }
{ "_id" : ObjectId("5918efb71cc609ca3f6d2d43"), "lts" : -1, "size" : 40, "src_addr" : "192.168.10.223", "msm_id" : 5027, "proto" : "UDP", "timestamp" : 1494806400, "msm_name" : "Traceroute", "fw" : 4770, "paris_id" : 14, "prb_id" : 18641, "af" : 4, "result" : [ { "result" : [ { "rtt" : 0.898, "ttl" : 255, "from" : "192.168.10.254", "size" : 28 }, { "rtt" : 0.861, "ttl" : 255, "from" : "192.168.10.254", "size" : 28 }, { "rtt" : 0.968, "ttl" : 255, "from" : "192.168.10.254", "size" : 28 } ], "hop" : 1 }, { "result" : [ { "rtt" : 1.433, "ttl" : 255, "from" : "192.168.10.254", "err" : "H", "size" : 28 }, { "x" : "*" }, { "rtt" : 1.309, "ttl" : 255, "from" : "192.168.10.254", "err" : "H", "size" : 28 } ], "hop" : 2 } ], "from" : "188.130.160.7", "endtime" : 1494806401, "type" : "traceroute", "dst_addr" : "139.162.27.28", "dst_name" : "139.162.27.28" }

```

## Running the analysis
There is two scripts for the analysis, rttAnalysis.py and routeAnalysis.py.
rttAnalysis.py monitors delays and reports abnormal delay changes. routeAnalysis.py monitors packets routes and reports forwarding anomalies.

### Setting parameters values

There is two ways to set the algorithms parameter values. 
The easy way: default parameters are hardcoded in the code. You can just change them and execute the script without command line arguments (e.g. python rttAnalysis.py).
For example, for rttAnalysis, find the following dictionary and change the values as you wish:
```
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
                "af": "",  # empty string means IPv4, set to "6" for IPv6
                "comment": "Study case for Emile (8.8.8.8) Nov. 2016",
                "prefixes": None
                }
```

The hard way: You can store your parameters in the mongo database and point to them by giving the ObjectId in the command line.

### Results
The code push results in the mongo database:
- the rttChanges collection contains all alarms for the delay changes
- the routeChanges collection contains all detected forwarding anomalies
- rttExperiments et routeExperiments keep the parameters values for each experiment


