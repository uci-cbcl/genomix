#!/bin/bash

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

PID_FILE=conf/ncs.pid
pid_re="^[0-9]+$"

for i in `cat conf/slaves`
do
   # ssh to the slave machine and capture its hostname and 
   SLAVE_PID=$( ssh -n $i "cd \"${GENOMIX_HOME}\" &>/dev/null; export JAVA_HOME=${JAVA_HOME} &>/dev/null ; export NCJAVA_OPTS="$NCJAVA_OPTS" &>/dev/null ; bin/startnc.sh $1" )
   if [[ "$SLAVE_PID" =~ $pid_re ]] ; then
     printf "slave: %s\t%s\n"  "$i" "$SLAVE_PID"
     printf "%s\t%s\n"  "$i" "$SLAVE_PID" >> $PID_FILE
   else
     echo "failed to start $i : expected PID, got $SLAVE_PID"
   fi
done
