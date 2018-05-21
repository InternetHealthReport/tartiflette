#!/bin/sh
for i in `seq 1 7`;
do
	python2 rttAnalysis.py stream & python2 routeAnalysis.py stream;
done  
