from ripe.atlas.cousteau import AtlasStream
import pymongo
import textwrap
import smtplib
import datetime
import emailConf
import time
import datetime 
import sys

client = pymongo.MongoClient("mongodb-iijlab")
db = client.atlas

nbTraces = 0
lastTimestamp = 0
currCollection = None
lastDownload = None
lastConnection = None

# builtin4 = [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010, 5011, 5012, 5013, 5014, 5015, 5016, 5017, 5018, 5019, 5020, 5021, 5022, 5024, 5025, 5026]
# anchoring4 = [2351480, 2244316, 3315653, 1962547, 2025970, 1437284, 1026355, 3361693, 1618360, 1446417, 2096533, 1698327, 1663325, 3058685, 1879037, 3044920, 1804069, 2417650, 1898895, 1026359, 1026363, 2957509, 3203677, 1875035, 1026367, 1695913, 1637582, 2439395, 1906512, 1026371, 1722410, 3103412, 1765533, 2457234, 2386536, 1842488, 2053744, 1042417, 1664873, 2924502, 1026375, 2055768, 1596619, 1042255, 1999831, 1768115, 3321322, 1861850, 1404703, 1864028, 1835778, 1696927, 1878008, 1583040, 2004731, 1413039, 1929377, 1026379, 1026383, 1907465, 1442070, 1042403, 1639699, 1421786, 2037869, 3062261, 1401947, 1043286, 2395060, 1423186, 3295749, 1577725, 1591156, 1026387, 1849605, 1402339, 1425314, 1791291, 1026391, 2923644, 1404333, 1769991, 1789806, 1790199, 1026395, 2464285, 1664730, 2063464, 3021099, 1402317, 1769335, 1765882, 1042245, 2067456, 1026399, 1790203, 1425318, 1042421, 2016840, 1880334, 2456804, 2398519, 2020876, 3217639, 1026403, 1891018, 1733341, 1962551, 2966283, 2037106, 2020834, 1404595, 1790232, 2841887, 1402084, 1929323, 1668851, 1575997, 1666914, 1835746, 1421259, 1589862, 1748022, 2435592, 1796567, 2904335, 1789561, 1789538, 1026407, 1842448, 1596739, 1845531, 3360589, 1217480, 1217480, 1043457, 1790332, 1698331, 1803655, 1726645, 1790207, 2394115, 1672156, 1039806, 1791209, 1665836, 1675583, 1768005, 2014981, 1042425, 1801217, 2339923, 1666038, 2398550, 2852012, 1566674, 1042407, 1817189, 2017398, 1806570, 1790944, 2339853, 2021326, 1672028, 1726568, 1791306, 2856282, 1990233, 1698335, 1603550, 1612282]
extraMsm = [1591146]

allmsm = []
# allmsm.extend(builtin4)
# allmsm.extend(anchoring4)
allmsm.extend(extraMsm)

class ConnectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

start = 1480302000
end = 1480305600


def sendMail(message):
    """
    Send an email with the given message.
    The destination/source addresses are defined in emailConf.
    """

    msg = textwrap.dedent("""\
        From: %s 
        To: %s 
        Subject: Atlas stream stopped on %s (UTC)! 
        %s 
    """ % (emailConf.orig, ",".join(emailConf.dest), datetime.datetime.utcnow(), message))

    # Send the mail
    server = smtplib.SMTP(emailConf.server)
    server.starttls()
    server.login(emailConf.username, emailConf.password)
    server.sendmail(emailConf.orig, emailConf.dest, msg)
    server.quit()


def on_result_response(*args):
    """
    Function called every time we receive a new traceroute.
    Store the traceroute in the corresponding Mongodb collection.
    """

    global lastTimestamp
    global currCollection
    global db
    global lastDownload
    global nbTraces

    lastDownload = datetime.datetime.now()
    trace = args[0]
    if trace["timestamp"] < 1480302000:
        print trace
    if lastTimestamp/(24*3600) != trace["timestamp"]/(24*3600) or currCollection is None:
        d = datetime.datetime.utcfromtimestamp(trace["timestamp"])
        coll = "traceroute_%s_%02d_%02d" % (d.year, d.month, d.day)
        currCollection = db[coll]
        currCollection.create_index("timestamp",background=True)
        lastTimestamp = trace["timestamp"]

    currCollection.insert_one(trace)

    nbTraces += 1
    print "\r %s traceroutes received" % nbTraces


def on_error(*args):
    print "got in on_error"
    print args

    raise ConnectionError("Error")


def on_connect(*args):
    print "got in on_connect"
    print args

def on_reconnect(*args):
    print "got in on_reconnect"
    print args

    raise ConnectionError("Reconnection")

def on_close(*args):
    print "got in on_close"
    print args

    raise ConnectionError("Closed")

def on_disconnect(*args):
    print "got in on_disconnect"
    print args

    raise ConnectionError("Disconnection")


def on_connect_error(*args):
    print "got in on_connect_error"
    print args

    raise ConnectionError("Connection Error")

def on_atlas_error(*args):
    print "got in on_atlas_error"
    print args


def on_atlas_unsubscribe(*args):
    print "got in on_atlas_unsubscribe"
    print args
    raise ConnectionError("Unsubscribed")



if __name__ == "__main__":

    # while True:
    now = datetime.datetime.utcnow()
    if start is None:
        start = time.mktime(datetime.datetime(now.year, now.month, now.day, now.hour-1, 59, 59).timetuple())
    if end is None:
        end = time.mktime(datetime.datetime(now.year, now.month, now.day, now.hour+1, 0, 0).timetuple())
    try:
        lastConnection = datetime.datetime.now()
        atlas_stream = AtlasStream()
        atlas_stream.connect()
        # Measurement results
        channel = "atlas_result"
        # Bind function we want to run with every result message received
        atlas_stream.socketIO.on("connect", on_connect)
        atlas_stream.socketIO.on("disconnect", on_disconnect)
        atlas_stream.socketIO.on("reconnect", on_reconnect)
        atlas_stream.socketIO.on("error", on_error)
        atlas_stream.socketIO.on("close", on_close)
        atlas_stream.socketIO.on("connect_error", on_connect_error)
        atlas_stream.socketIO.on("atlas_error", on_atlas_error)
        atlas_stream.socketIO.on("atlas_unsubscribed", on_atlas_unsubscribe)
        # Subscribe to new stream 
        atlas_stream.bind_channel(channel, on_result_response)
        
        for msm in allmsm:
            stream_parameters = {"type": "traceroute", "buffering":True, "equalsTo":{"af": 4}, "startTime":start, "endTime":end, "msm": msm, "speed":1}
            # stream_parameters = {"type": "traceroute", "buffering":True, "equalsTo":{"af": 4}, "greaterThan":{"timestamp":start}, "lessThan":{"timestamp":end}, "msm": msm}
            atlas_stream.start_stream(stream_type="result", **stream_parameters)

        # Run forever
        atlas_stream.timeout(seconds=3600)
        # Shut down everything
        atlas_stream.disconnect()

    except ConnectionError as e:
        now = datetime.datetime.utcnow()
        print "%s: %s" % (now, e)
        print "last download: %s" % lastDownload
        print "last connection: %s" % lastConnection
        atlas_stream.disconnect()

        # Wait a bit if the connection was made less than a minute ago
        if lastConnection + datetime.timedelta(60) > now:
            time.sleep(60) 
        print "Go back to the loop and reconnect"

    except Exception as e: 
        save_note = "Exception dump: %s : %s." % (type(e).__name__, e)
        exception_fp = open("dump_%s.err" % datetime.datetime.now(), "w")
        exception_fp.write(save_note) 
        sendMail(save_note)
        sys.exit()
