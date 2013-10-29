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

if [[ $# != 1 || ("$1" != "HYRACKS" && "$1" != "PREGELIX") ]]; then
    echo "please provide a cluster type as HYRACKS or PREGELIX! (saw $1)" 1>&2
    exit 1
fi
if [ $1 == "HYRACKS" ]; then
    MY_HOME="$GENOMIX_HOME/hyracks"
    CMD="\"${MY_HOME}/bin/hyracksnc\""
else 
    MY_HOME="$GENOMIX_HOME/pregelix"
    CMD="\"${MY_HOME}/bin/pregelixnc\""

    # cleanup temporary stores dirs
    . "$MY_HOME/conf/stores.properties"
    for store_dir in $(echo $store | tr "," "\n"); do
        rm -rf $store_dir
        mkdir -p $store_dir
    done
fi

MY_NAME=`hostname`
#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CCHOST=`ssh -n ${CCHOST_NAME} "${GENOMIX_HOME}/bin/getip.sh"`

# import cluster properties
. "$MY_HOME/conf/cluster.properties"

#Clean up temp dirs
shopt -s extglob
rm -rf $NCTMP_DIR/\(!logs\)  # remove all except default log dir
mkdir -p $NCTMP_DIR
mkdir -p $NCLOGS_DIR

#Clean up I/O working dir
for io_dir in $(echo $IO_DIRS | tr "," "\n"); do
	rm -rf $io_dir
	mkdir -p $io_dir
done

IPADDR=`bin/getip.sh`
#echo $IPADDR

#Get node ID
NODEID=`hostname | cut -d '.' -f 1`

export JAVA_HOME=$JAVA_HOME
export JAVA_OPTS=$NCJAVA_OPTS

#Enter the temp dir
cd $NCTMP_DIR

CMD+=" -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -result-ip-address $IPADDR -node-id $NODEID -iodevices \"${IO_DIRS}\""

printf "\n\n\n********************************************\nStarting NC with command %s\n\n" "$CMD" >> "$NCLOGS_DIR"/$NODEID.log

#Launch nc
eval "$CMD >> \"$NCLOGS_DIR/$NODEID.log\" 2>&1  &"
PID=$!
sleep 1
MISSING=false
kill -0 $PID || MISSING=true  # check if the nc process is still running

if [ "$MISSING" == "true" ]; then
    echo "Failure detected when starting NC!" 1>&2
    tail -15 "$NCLOGS_DIR/$NODEID.loglog" 1>&2
    exit 1
else
    echo $PID
fi
