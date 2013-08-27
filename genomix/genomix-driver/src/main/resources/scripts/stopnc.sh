#!/bin/bash

hostname

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

. conf/cluster.properties


if [ "$1" == "hyracks" ]; then
  NCTYPE="hyracksnc"
elif [ "$1" == "pregelix" ]; then
  NCTYPE="pregelixnc"
else
  echo "unknown NC type $1" 1>&2
  exit 1
fi


#Kill process
PID=`jps -lvm | grep "Dapp.name=$NCTYPE" | awk '{print $1}'`

if [ "$PID" == "" ]; then
  PID=`ps -ef|grep ${USER}|grep java|grep "Dapp.name=$NCTYPE"|awk '{print $2}'`
fi

if [ "$PID" == "" ]; then
  PID=`ps -ef|grep ${USER}|grep java|grep 'hyracks'|awk '{print $2}'`
fi

if [ "$PID" == "" ]; then
  PID=`ps -ef|grep ${USER}|grep java|grep 'hyracks'|awk '{print $2}'`
fi

if [ "$PID" == "" ]; then
  USERID=`id | sed 's/^uid=//;s/(.*$//'`
  PID=`ps -ef|grep ${USERID}|grep java|grep "Dapp.name=$NCTYPE"|awk '{print $2}'`
fi

echo $PID
kill -9 $PID || echo "Couldn't find process with PID $PID"

#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir/*
done

#Clean up NC temp dir
rm -rf $NCTMP_DIR/*
