#!/bin/bash
set +e  # never stop if a subshell fails
set +o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

PID_FILE=conf/ncs.pid

if [ ! -e $PID_FILE ] ; then
  echo "No pid file for NC's! (expected $GENOMIX_HOME/$PID_FILE)" 
  exit 1
else
  while IFS=$'\t' read -r -a SLAVE_PID
  do
     ssh -n ${SLAVE_PID[0]} "cd \"$GENOMIX_HOME\"; bin/stopnc.sh ${SLAVE_PID[1]}"
     if [ $? == 0 ] ; then
       echo "Stopped slave: ${SLAVE_PID[0]}"$'\t'"${SLAVE_PID[1]}"
     else
       echo "Failed to stop slave:" ${SLAVE_PID[0]} ${SLAVE_PID[1]}
     fi
  done < $PID_FILE
  rm $PID_FILE
fi


