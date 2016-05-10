# ripeAtlasDetector

Pinpointing delay and forwarding anomalies from large-scale traceroute measurements.

Our results with the RIPE Atlas traceroute data is available here: http://romain.iijlab.net/iwr/reports/


## Requirements
- pandas
- matplotlib
- pymongo
- pygeoip
- statsmodels


## Easy way to import data from files
zcat 2015-06-0*.gz | grep -f scripts/msmIds.txt | mongoimport --db atlas
--collection traceroute

This product includes GeoLite data created by MaxMind, available from http://www.maxmind.com
