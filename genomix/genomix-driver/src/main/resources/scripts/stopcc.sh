#!/bin/bash

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

. conf/cluster.properties

PID_FILE=conf/cc.pid

if [ ! -e $PID_FILE ] ; then
  echo "No pid file for CC! (expected $GENOMIX_HOME/$PID_FILE)" 
  exit 1
else
  PID=`cat $PID_FILE`
  kill -9 $PID
  rm $PID_FILE
  echo "Stopped CC on master: "`hostname`$'\t'$PID
fi

#Clean up CC temp dir but keep the default logs directory
shopt -s extglob
rm -rf $CCTMP_DIR/!(logs)


