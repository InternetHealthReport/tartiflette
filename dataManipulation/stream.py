from ripe.atlas.cousteau import AtlasStream
import pymongo
import textwrap
import smtplib
import datetime
import emailConf
import time

client = pymongo.MongoClient("mongodb-iijlab")
db = client.atlas

lastTimestamp = 0
currCollection = None

def sendMail(message):

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

    trace = args[0]
    if lastTimestamp/(24*3600) != trace["timestamp"]/(24*3600) or currCollection is None:
        d = datetime.datetime.utcfromtimestamp(trace["timestamp"])
        print "getting data for %s" % d
        coll = "traceroute_%s_%02d_%02d" % (d.year, d.month, d.day)
        currCollection = db[coll]
        currCollection.create_index("timestamp",background=True)
        lastTimestamp = trace["timestamp"]

    currCollection.insert_one(trace)


if __name__ == "__main__":

    try:
        while True:
            atlas_stream = AtlasStream()
            atlas_stream.connect()
            # Measurement results
            channel = "result"
            # Bind function we want to run with every result message received
            atlas_stream.bind_channel(channel, on_result_response)
            # Subscribe to new stream 
            stream_parameters = {"type": "traceroute"}
            atlas_stream.start_stream(stream_type="result", **stream_parameters)

            # Run forever
            atlas_stream.timeout(seconds=None)
            # Shut down everything
            atlas_stream.disconnect()

            print "Disconnection! will try to reconnect in 1 second..."
            time.sleep(1) 

        # This script should never end, send an email if it does
        save_note = "normal exit"
    except Exception as e: 
        save_note = "Exception dump: %s : %s." % (type(e).__name__, e)
    finally:
        exception_fp = open("dump_%sUTC.err" % datetime.datetime.utcnow(), "w")
        exception_fp.write(save_note) 
        sendMail(save_note)
