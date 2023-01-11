import sys
import os 
from datetime import datetime
from datetime import timedelta
from datetime import date
from dateutil import relativedelta
from ripe.atlas.cousteau import AtlasResultsRequest 
import json
import pymongo
import gzip
import msgpack
import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass


def downloadData(start, end, msmTypes = ["builtin", "anchor"], afs = [4, 6], reverse=False, split=0, timeWindow = timedelta(minutes=24*60) ):
    errors = []

    producer = Producer({'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
        'queue.buffering.max.messages': 10000000,
        'queue.buffering.max.kbytes': 2097151,
        'linger.ms': 200,
        'batch.num.messages': 1000000,
        'message.max.bytes': 999000,
        'default.topic.config': {'compression.codec': 'snappy'}})
    admin_client = AdminClient({'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092'})

    created_topics = list(admin_client.list_topics().topics.keys())
    
    # Get measurments results
    for af in afs:
        for msmType in msmTypes:
            # Read the corresponding id file
            if reverse:
                msmIds = reversed(open("../data/%sMsmIdsv%s.txt" % (msmType, af), "r").readlines())
            else:
                msmIds = open("../data/%sMsmIdsv%s.txt" % (msmType, af), "r").readlines()

            msmIds = msmIds[int(len(msmIds)*split):]

            for line in msmIds: 
                msmId = int(line.partition(":")[2])

                currDate = start
                while currDate+timeWindow<end:
                    path = "../data/ipv%s/%s/%s/%s" % (af,msmType,currDate.year, currDate.month)
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

                        if is_success :
                                # results = list(results)
                            # else:
                                # Output file
                                fi = gzip.open("%s/%s_msmId%s.json.gz" % (path, currDate, msmId) ,"wb")
                                print("Storing data for %s measurement id %s" % (currDate, msmId))
                                results_data = json.dumps(results)
                                fi.write(results_data.encode('utf-8'))
                                fi.close()
                            # if storage == "mongo":
                                if len(results)==0:
                                    continue
                                print("Sending data to Kafka Cluster")
                                if af == 4:
                                    #TODO store data in traceroute4
                                    topic = "traceroute_%s_%02d_%02d" % (currDate.year, 
                                                                currDate.month, currDate.day)
                                elif af == 6:
                                    topic = "traceroute6_%s_%02d_%02d" % (currDate.year, 
                                                                currDate.month, currDate.day)
                                
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
                                    
                                for traceroute in results:
                                    try:
                                        producer.produce(
                                            topic, 
                                            msgpack.packb(traceroute, use_bin_type=True),
                                            traceroute['msm_id'].to_bytes(8, byteorder='big'),
                                            callback=delivery_report,
                                            timestamp = traceroute.get('timestamp')*1000
                                        )
                                        producer.poll(0)
                                    except BufferError:
                                        print("Local queue is full")
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
                                

                        else:
                            errors.append("%s: msmId=%s" % (currDate, msmId))
                            print("error: %s: msmId=%s" % (currDate, msmId))

                    except ValueError:
                        errors.append("%s: msmId=%s" % (currDate, msmId))
                        print("error: %s: msmId=%s" % (currDate, msmId))

                    finally:
                        currDate += timeWindow


    if errors:
        print("Errors with the following parameters:")
        print(errors)


if __name__ == "__main__":
    # download yesterday's data
    if len(sys.argv) < 5:
        print("usage: %s year month (builtin, anchor) af" % sys.argv[0])
        sys.exit()

    # logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, filename='../logs/download-data.log' , 
            level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)
    
    start = datetime(int(sys.argv[1]), int(sys.argv[2]), 1)
    end= start + relativedelta.relativedelta(months=1)

    downloadData(start, end, [sys.argv[3]], [int(sys.argv[4])] )
