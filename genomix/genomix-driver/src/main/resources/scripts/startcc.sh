#!/bin/bash
#------------------------------------------------------------------------
# Copyright 2009-2013 by The Regents of the University of California
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License from
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

if [[ $# != 1 || ( "$1" != "HYRACKS" && "$1" != "PREGELIX" ) ]]; then
    echo "please provide a cluster type as HYRACKS or PREGELIX! (saw $1)" 1>&2
    exit 1
fi
if [ $1 == "HYRACKS" ]; then
    MY_HOME="$GENOMIX_HOME/hyracks"
    CMD="\"${MY_HOME}/bin/hyrackscc\""
else 
    MY_HOME="$GENOMIX_HOME/pregelix"
    CMD="\"${MY_HOME}/bin/pregelixcc\""
fi

if [ -e "$MY_HOME"/cc.pid ] ; then
  echo "existing CC pid found in $MY_HOME/cc.pid... Not starting a new CC!"
  exit 1
fi

#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CCHOST=`ssh -n ${CCHOST_NAME} "${GENOMIX_HOME}/bin/getip.sh"`

# import cluster properties
. "$MY_HOME/conf/cluster.properties"

#Clean the tmp and logs dirs
shopt -s extglob
rm -rf $CCTMP_DIR/\(!logs\)  # remove all except default log dir
mkdir -p $CCTMP_DIR
mkdir -p $CCLOGS_DIR

#Export JAVA_HOME and JAVA_OPTS
export JAVA_HOME=$JAVA_HOME
export JAVA_OPTS=$CCJAVA_OPTS

cd $CCTMP_DIR
#Prepare cc script
CMD+=" -max-heartbeat-lapse-periods 999999 -default-max-job-attempts 0 -client-net-ip-address $CCHOST -cluster-net-ip-address $CCHOST"

if [ -n "$CC_CLIENTPORT" ]; then
  CMD="$CMD -client-net-port $CC_CLIENTPORT"
fi
if [ -n "$CC_CLUSTERPORT" ]; then
  CMD="$CMD -cluster-net-port $CC_CLUSTERPORT"
fi
if [ -n "$CC_HTTPPORT" ]; then
  CMD="$CMD -http-port $CC_HTTPPORT"
fi
if [ -n "$JOB_HISTORY_SIZE" ]; then
  CMD="$CMD -job-history-size $JOB_HISTORY_SIZE"
fi
if [ -f "${GENOMIX_HOME}/conf/topology.xml"  ]; then
CMD="$CMD -cluster-topology \"${GENOMIX_HOME}/conf/topology.xml\""
fi

#Launch cc script
printf "\n\n\n********************************************\nStarting CC with command %s\n\n" "$CMD" >> "$CCLOGS_DIR"/cc.log
#eval "$CMD >>\"$CCLOGS_DIR/cc.log\" 2>&1 &"

# start the cc process and make sure it exists after 1 second
eval "$CMD >>\"$CCLOGS_DIR/cc.log\" 2>&1 &"
PID=$!
sleep 1
MISSING=false
kill -0 $PID || MISSING=true  # check if the nc process is still running

if [ "$MISSING" == "true" ]; then
    echo "Failure detected when starting CC!" 1>&2
    tail -15 "$CCLOGS_DIR/cc.log" 1>&2
    exit 1
else
    echo "master: "`hostname`$'\t'$PID
    echo $PID > "$MY_HOME/cc.pid"
fi

