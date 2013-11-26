#/bin/bash

if [ $# == 0 ] ; then
	echo "Please provide the input log file. Ex. $0 ...log"
	exit 1
fi

INPUT=$1

echo "Generating the time of each step from $INPUT..."

less $INPUT | grep "Building the graph took" | awk '{$6 = $6/1000"s"; print $2,"\t",$6}'

less $INPUT | grep "Finished job" | awk '{$6 = $6/1000"s"; print $4,"\t",$6}'
