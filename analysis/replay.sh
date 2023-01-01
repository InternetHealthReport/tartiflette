#!/bin/sh
for i in `seq 1 7`;
do
	python3 rttAnalysis.py & python3 routeAnalysis.py;
done  
