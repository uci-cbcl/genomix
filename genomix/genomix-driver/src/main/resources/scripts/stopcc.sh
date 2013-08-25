#!/bin/bash

hostname

GENOMIX_HOME="$( dirname "$( dirname "$( readlink -e "${BASH_SOURCE[0]}" )" )" )"  # parent dir of absolute script path
cd "$GENOMIX_HOME"

. conf/cluster.properties

#Kill process
PID=`ps -ef|grep ${USER}|grep java|grep 'Dapp.name=genomixcc'|awk '{print $2}'`

if [ "$PID" == "" ]; then
    PID=`ps -ef|grep ${USER}|grep java|grep 'hyracks'|awk '{print $2}'`
fi

if [ "$PID" == "" ]; then
    USERID=`id | sed 's/^uid=//;s/(.*$//'`
    PID=`ps -ef|grep ${USERID}|grep java|grep 'Dapp.name=genomixcc'|awk '{print $2}'`
fi

echo $PID
kill -9 $PID || echo "Couldn't find process with PID $PID"

#Clean up CC temp dir
rm -rf $CCTMP_DIR/*
