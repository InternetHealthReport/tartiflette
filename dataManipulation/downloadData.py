import os 
from datetime import datetime
from datetime import timedelta
from ripe.atlas.cousteau import AtlasResultsRequest 
import json
import pymongo

# Measurments IDs
builtinIdv4 = range(5001,5027)

# dates
start = datetime(2015, 11, 15, 23, 45)
end = datetime(2015, 12, 6, 23, 45)
timeWindow = timedelta(minutes=30)

errors = []

storage = "mongo"
if storage == "mongo":
    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas

# Get measurments results
for msmId in builtinIdv4:

    currDate = start
    while currDate+timeWindow<end:

        try:
            print("%s:  measurement id %s" % (currDate, msmId) )
            if os.path.exists("../data/%s_msmId%s.json" % (currDate, msmId)):
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
                    fi = open("../data/%s_msmId%s.json" % (currDate, msmId) ,"w")
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
