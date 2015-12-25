# ripeAtlasDetector


## Easy way to import data from files
zcat 2015-06-0*.gz | grep -f scripts/msmIds.txt | mongoimport --db atlas
--collection traceroute

This product includes GeoLite data created by MaxMind, available from http://www.maxmind.com
