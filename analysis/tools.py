import re
import numpy as np
from pymongo import MongoClient
import string


# https://en.wikipedia.org/wiki/Private_network
priv_lo = re.compile("^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_24 = re.compile("^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_20 = re.compile("^192\.168\.\d{1,3}.\d{1,3}$")
priv_16 = re.compile("^172.(1[6-9]|2[0-9]|3[0-1]).[0-9]{1,3}.[0-9]{1,3}$")

def isPrivateIP(ip):

    return priv_lo.match(ip) or priv_24.match(ip) or priv_20.match(ip) or priv_16.match(ip)

def str2filename(txt):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    return "".join( c for c in txt if c in valid_chars)

def mad(arr):
    """ Median Absolute Deviation: a "Robust" version of standard deviation.
        Indices variabililty of the sample.
        https://en.wikipedia.org/wiki/Median_absolute_deviation 
    """
    arr = np.ma.array(arr).compressed() # should be faster to not use masked arrays.
    med = np.median(arr)
    return np.median(np.abs(arr - med))


def connect_mongo(host="mongodb-iijlab", port=27017, db="atlas", username="", password=""):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]

# TODO: remove the following:

# from math import sqrt

# def wilsonConfInt(ups, downs):
    # n = ups + downs

    # if n == 0:
        # return 0

    # z = 1.0 #1.44 = 85%, 1.96 = 95%
    # phat = float(ups) / n
    # return ((phat + z*z/(2*n) - z * sqrt((phat*(1-phat)+z*z/(4*n))/n))/(1+z*z/n))

# from scipy.stats import beta

# def binom_interval(success, total, confint=0.95):
    # quantile = (1 - confint) / 2.
    # lower = beta.ppf(quantile, success, total - success + 1)
    # upper = beta.ppf(1 - quantile, success + 1, total - success)
    # return (lower, upper)


class RingBuffer():
    "A 1D ring buffer using numpy arrays"
    def __init__(self, length):
        self.data = np.zeros(length, dtype='f')
        self.index = 0

    def extend(self, x):
        "adds array x to ring buffer"
        x_index = (self.index + np.arange(x.size)) % self.data.size
        self.data[x_index] = x
        self.index = x_index[-1] + 1

    def get(self):
        "Returns the first-in-first-out data in the ring buffer"
        idx = (self.index + np.arange(self.data.size)) %self.data.size
        return self.data[idx]

def ringbuff_numpy_test():
    ringlen = 100000
    ringbuff = RingBuffer(ringlen)
    for i in range(40):
        ringbuff.extend(np.zeros(10000, dtype='f')) # write
        ringbuff.get() #read
