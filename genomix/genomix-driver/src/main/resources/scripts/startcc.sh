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
# set -x

genomix_home="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$genomix_home"

if [[ $# != 1 || ( "$1" != "HYRACKS" && "$1" != "PREGELIX" ) ]]; then
    echo "please provide a cluster type as HYRACKS or PREGELIX! (saw $1)" 1>&2
    exit 1
fi
if [ $1 == "HYRACKS" ]; then
    my_home="$genomix_home/hyracks"
    cmd=( "${my_home}/bin/hyrackscc" )
else 
    my_home="$genomix_home/pregelix"
    cmd=( "${my_home}/bin/pregelixcc" )
fi

if [ -e "$my_home/cc.pid" ] ; then
  echo "existing CC pid found in $my_home/cc.pid... Not starting a new CC!"
  exit 1
fi

#Get the IP address of the cc
cchost_name=`cat conf/master`
cchost=`ssh -n ${cchost_name} "${genomix_home}/bin/getip.sh"`

# import cluster properties
. "$my_home/conf/cluster.properties"

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
cmd+=( -max-heartbeat-lapse-periods 999999 -default-max-job-attempts 0 
       -client-net-ip-address $cchost -cluster-net-ip-address $cchost )

if [ -n "$CC_CLIENTPORT" ]; then
    cmd+=( -client-net-port $CC_CLIENTPORT )
fi
if [ -n "$CC_CLUSTERPORT" ]; then
    cmd+=( -cluster-net-port $CC_CLUSTERPORT )
fi
if [ -n "$CC_HTTPPORT" ]; then
    cmd+=( -http-port $CC_HTTPPORT )
fi
if [ -n "$JOB_HISTORY_SIZE" ]; then
    cmd+=( -job-history-size $JOB_HISTORY_SIZE )
fi
if [ -f "${genomix_home}/conf/topology.xml"  ]; then
    cmd+=( -cluster-topology "${genomix_home}/conf/topology.xml" )
fi

#Launch cc script
printf "\n\n\n********************************************\nStarting CC with command %s\n\n" "${cmd[*]}" >> "$CCLOGS_DIR/cc.log"
#eval "$cmd >>\"$CCLOGS_DIR/cc.log\" 2>&1 &"

# start the cc process and make sure it exists after a few seconds
eval "${cmd[@]} >> \"$CCLOGS_DIR/cc.log\" 2>&1 &"
server_pid=$!
sleep 1
set +e
kill -0 $server_pid
is_alive=$?
set -e
if (($is_alive == 0)); then
    echo "master: $cchost_name"$'\t'$server_pid
    echo $server_pid > "$my_home/cc.pid"
else
    echo >&2 "Failure detected when starting CC! (should have PID $server_pid)"
    tail >&2 -15 "$CCLOGS_DIR/cc.log"
    exit 1
fi
exit 0
