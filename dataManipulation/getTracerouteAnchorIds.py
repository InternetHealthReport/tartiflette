import sys
from ripe.atlas.cousteau import AtlasRequest 
from ripe.atlas.cousteau import MeasurementRequest


def getAnchorMsmPage(page=1):
    url_path = "/api/v2/anchor-measurements?page=%s" % page

    request = AtlasRequest(**{"url_path": url_path})
    (is_success, response) = request.get()

    if not is_success:
            print "Error could not get all anchors data!"
            return []
    else:
        if response["next"] is None:
            return response["results"], None
        else:
            return response["results"], page+1


def getAllAnchorMsm():
    allIds = []
    ids, page = getAnchorMsmPage()
    while not page is None:
        allIds.extend(ids)
        ids, page = getAnchorMsmPage(page)

    allIds.extend(ids)

    return allIds


def storeTracerouteIds():

    sys.stdout.write("Getting all anchoring measurement ids...\n")
    allMsm = getAllAnchorMsm()
    sys.stdout.write("%s anchoring measurements found!\n" % len(allMsm))

    v4file = open("anchorMsmIdsv4.txt","w")
    v6file = open("anchorMsmIdsv6.txt","w")
    count = 0

    sys.stdout.write("Filtering out traceroute measurements...\n")
    # filters = {"type": "traceroute", "is_public": True}
    # measurements = MeasurementRequest(**filters)
    
    for m in allMsm:
        request = AtlasRequest(**{"url_path": m["measurement"][len(u"https://atlas.ripe.net"):]})
        (is_success, response) = request.get()

        if not is_success:
            print "error cannot get the measurment details!?\n %s" % m
            continue

        if response["type"] != "traceroute":
            continue

        count += 1
        sys.stdout.write("\r%s anchoring measurments found" % count)
        sys.stdout.flush()
        if response["af"] == 4:
            v4file.write('"msm_id":%s\n' % response["id"])
            v4file.flush()
        elif response["af"] == 6:
            v6file.write('"msm_id":%s\n' % response["id"])
            v6file.flush()
        else:
            print "Unknown address family!! \n %s" % m


storeTracerouteIds()
