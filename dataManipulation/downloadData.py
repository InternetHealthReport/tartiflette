import os 
from datetime import datetime
from datetime import timedelta
from ripe.atlas.cousteau import AtlasResultsRequest 
import json
import pymongo
import gzip

# Measurments IDs
builtinIdv4 = [5016] #range(5001,5027)

# dates
start = datetime(2015, 10, 14, 0, 0)
end = datetime(2015, 10, 26, 0, 0)
timeWindow = timedelta(minutes=60)

errors = []

# storage = "mongo"
storage = "fs"
if storage == "mongo":
    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas

# Get measurments results
for msmId in builtinIdv4:

    currDate = start
    while currDate+timeWindow<end:
        path = "../data/%s/%s" % (currDate.year, currDate.month)
        try:
            print("%s:  measurement id %s" % (currDate, msmId) )
            if not os.path.exists(path):
                os.makedirs(path)
            if os.path.exists("%s/%s_msmId%s.json.gz" % (path, currDate, msmId)):
                continue

            kwargs = {
                "msm_id": msmId,
                "start": currDate,
                "stop": currDate+timeWindow,
                # "probe_ids": [1,2,3,4]
            }

            is_success, results = AtlasResultsRequest(**kwargs).create()

            if is_success:
                if storage == "mongo":
                    collection = "traceroute_%s_%02d_%02d" % (currDate.year, 
                                                currDate.month, currDate.day)
                    col = db[collection]
                    col.insert_many(results)
                    col.create_index({"timestamp": 1 })       

                else:
                    # Output file
                    fi = gzip.open("%s/%s_msmId%s.json.gz" % (path, currDate, msmId) ,"wb")
                    print("Storing data for %s measurement id %s" % (currDate, msmId) )
                    json.dump(results, fi)
                    fi.close()
            else:
                errors.append("%s: msmId=%s" % (currDate, msmId))

        except ValueError:
            errors.append("%s: msmId=%s" % (currDate, msmId))

        finally:
            currDate += timeWindow


if errors:
    print("Errors with the following parameters:")
    print(errors)
