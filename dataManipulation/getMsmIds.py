from ripe.atlas.cousteau import MeasurementRequest
from collections import defaultdict

filters = {"status": 2, "type": "traceroute", "af": 4}
measurements = MeasurementRequest(**filters)

count = 0
stat = {}
msmIds = []
for msm in measurements:
    if msm["interval"] <= 1800 and (msm["participant_count"] is None or msm["participant_count"]>100) and not msm["target_ip"].startswith("184.164.224"):
        # for k,v in msm.
        print(msm)
        msmIds.append(msm["id"])
        count += 1

# Print total count of found measurements
print("Found %s interesting measurement out of %s" % (count, measurements.total_count))
print(list(set(msmIds)))
