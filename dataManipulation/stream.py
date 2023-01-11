from ripe.atlas.cousteau import AtlasStream
import pymongo
import textwrap
import smtplib
from configs import emailConf
import time
import datetime 
import sys
import json
import logging
import msgpack
from email.mime.text import MIMEText
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

class ConnectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def sendMail(message):
    """
    Send an email with the given message.
    The destination/source addresses are defined in emailConf.
    """

    msg = MIMEText(message)
    msg["Subject"] = "Atlas stream stopped on %s (UTC)!" % datetime.datetime.utcnow()
    msg["From"] = emailConf.orig 
    msg["To"] = emailConf.dest 

    # Send the mail
    server = smtplib.SMTP(emailConf.server, port=587)
    server.starttls()
    server.login(emailConf.username, emailConf.password)
    server.sendmail(emailConf.orig, emailConf.dest, msg.as_string())
    server.quit()


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass

def on_result_response(*args):
    """
    Function called every time we receive a new traceroute.
    Store the traceroute in the corresponding Mongodb collection.
    """
    global topic
    global lastTimestamp
    global lastDownload
    global producer
    global admin_client
    global created_topics
    print(json.dumps(args[0], indent=4, sort_keys=True))
    lastDownload = datetime.datetime.now()
    traceroute = args[0]
    if lastTimestamp/(24*3600) != traceroute["timestamp"]/(24*3600) or topic is None:
        d = datetime.datetime.utcfromtimestamp(traceroute["timestamp"])
        topic = "traceroute_%s_%02d_%02d" % (d.year, d.month, d.day)
        lastTimestamp = traceroute["timestamp"]

    if topic not in created_topics:
        topic_list = [NewTopic(topic, num_partitions=3, replication_factor=2)]
        created_topic = admin_client.create_topics(topic_list)
        for topic, f in created_topic.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
        created_topics.append(topic)
    try:
        logging.debug('going to produce something')
        producer.produce(
            topic, 
            msgpack.packb(traceroute, use_bin_type=True),
            traceroute['msm_id'].to_bytes(8, byteorder='big'),
            callback=delivery_report,
            timestamp = traceroute.get('timestamp')*1000
        )
        logging.debug('produced something')
        producer.poll(0)
    except BufferError:
        logging.error('Local queue is full ')
        producer.flush()
        producer.produce(
            topic, 
            msgpack.packb(traceroute, use_bin_type=True),
            traceroute['msm_id'].to_bytes(8, byteorder='big'),
            callback=delivery_report,
            timestamp = traceroute.get('timestamp')*1000
        )
        producer.poll(0)
    producer.flush()

def on_error(*args):
    logging.error(f"got in on_error: {args}")
    raise ConnectionError("Error")


def on_connect(*args):
    logging.debug(f"got in on_connect: {args}")

def on_reconnect(*args):
    logging.debug(f"got in on_reconnect: {args}")
    raise ConnectionError("Reconnection")

def on_close(*args):
    logging.debug(f"got in on_close: {args}")
    
    raise ConnectionError("Closed")

def on_disconnect(*args):
    logging.debug(f"got in on_disconnect: {args}")
    raise ConnectionError("Disconnection")


def on_connect_error(*args):
    logging.error(f"got in on_connect_error: {args}")
    raise ConnectionError("Connection Error")

def on_atlas_error(*args):
    logging.error(f"got in on_atlas_error: {args}")

def on_atlas_unsubscribe(*args):
    logging.debug(f"got in on_atlas_unsubscribe: {args}")
    raise ConnectionError("Unsubscribed")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: %s id0 [ id1 [id2 [...]]]" % sys.argv[0])
        sys.exit()

    # logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, filename='../logs/stream-ihr-kafka-traceroute.log' , 
            level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)
    
    #Start time of this script, we'll try to get it working for 1 hour
    starttime = datetime.datetime.now()

    producer = Producer({'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
            'queue.buffering.max.messages': 10000000,
            'queue.buffering.max.kbytes': 2097151,
            'linger.ms': 200,
            'batch.num.messages': 1000000,
            'message.max.bytes': 999000,
            'default.topic.config': {'compression.codec': 'snappy'}})    
    admin_client = AdminClient({'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092'})
    created_topics = list(admin_client.list_topics(timeout=5).topics.keys())

    lastTimestamp = 0
    lastDownload = None
    topic = None
    lastConnection = None

    allmsm = []
    for msmId in sys.argv[1:]:
        allmsm.append(int(msmId))

    while (datetime.datetime.now()-starttime).seconds < 3600:
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
                stream_parameters = {"type": "traceroute", "buffering":True, "msm": msm}
                atlas_stream.start_stream(stream_type="result", **stream_parameters)

            # Run for 1 hour
            logging.debug("start stream for msm ids: %s" % allmsm)
            atlas_stream.timeout(seconds=3600-(datetime.datetime.now()-starttime).seconds)
            # Shut down everything
            atlas_stream.disconnect()
            break

        except ConnectionError as e:
            now = datetime.datetime.utcnow()
            logging.error("Connection error: " % e)
            logging.debug("%s: %s" % (now, e))
            logging.error("last download: %s" % lastDownload)
            logging.error("last connection: %s" % lastConnection)
            atlas_stream.disconnect()

            # Wait a bit if the connection was made less than a minute ago
            if lastConnection + datetime.timedelta(60) > now:
                time.sleep(60) 
            logging.debug("Go back to the loop and reconnect")

        except Exception as e: 
            save_note = "Exception dump: %s : %s.\nCommand: %s" % (type(e).__name__, e, sys.argv)
            exception_fp = open("dump_%s.err" % datetime.datetime.now(), "w")
            exception_fp.write(save_note) 
            sendMail(save_note)
            sys.exit()
            
        