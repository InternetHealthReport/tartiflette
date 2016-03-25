import os
import sys
import json
import glob
import pymongo
import datetime
import gzip

# def importMongo(path,collection, db=None):

    # if db is None:
        # client = pymongo.MongoClient("mongodb-iijlab")
        # db = client.atlas
    # col = db[collection]

    # for filename in glob.glob(path):
        # fi = open(filename)
        # data = json.load(fi)

        # print "%s: %s documents" % (filename, len(data))
        # col.insert_many(data)

def importRangeOfDates(start, end, msmType, af=""):

    if msmType != "builtin" and msmType != "anchor":
        print "measurement type unknown!"
        return
   
    #TODO allow only af==4 or 6
    # thus data is stored in collections traceroute4 or tracereoute6
    if af=="":
        aflabel = "4"
    else:
        aflabel = af

    client = pymongo.MongoClient("mongodb-iijlab")
    db = client.atlas

    nbDays = (end-start).days
    dateRange = [start+datetime.timedelta(x) for x in range(nbDays)]

    for date in dateRange:
        # colName = "traceroute_{year}_{month:02d}_{day:02d}".format(
        colName = "traceroute{af}_{year}_{month:02d}_{day:02d}".format( af=af, year=date.year, month=date.month, day=date.day)
        col = db[colName]

        if date < datetime.datetime(2015,10,13):
            # Data from Emile
            filename = "/export/ripe/atlas/traceroute/{year}-{month:02d}-{day:02d}.gz".format(
                                year=date.year, month=date.month, day=date.day)

            msmIdFile = "./%sMsmIdsv%s.txt" % (msmType, aflabel)

            os.system("zcat %s | grep -f %s | mongoimport -h mongodb-iijlab -d atlas -c %s " % (
                                                        filename, msmIdFile, colName))

            col.create_index("timestamp", background=True)

        else:
            pass # data are stored when downloaded
            # Downloaded data
            path = "../data/ipv{aflabel}/{msmType}/{year}/{month}/{year}-{month:02d}-{day:02d}*.json.gz".format(
                aflabel=aflabel, msmType=msmType, year=date.year, month=date.month, day=date.day)

            files = glob.glob(path)
            files.sort() # insert data in chronological order
            for filename in files:
                fi = gzip.open(filename)
                data = json.load(fi)
                if len(data):
                    print filename
                    col.insert_many(data)
                else:
                    print "%s is empty!" % filename
            col.create_index("timestamp", background=True)


if __name__ == "__main__":
    pass
    # Don't use this:
    # if len(sys.argv) < 3:
        # print "usage: %s filesPattern collection" % sys.argv[0]
    # else:
        # importMongo(sys.argv[1], sys.argv[2])

