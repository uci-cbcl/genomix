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

genomix_home="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$genomix_home"

if [[ $# != 1 || ("$1" != "HYRACKS" && "$1" != "PREGELIX") ]]; then
    echo "please provide a cluster type as HYRACKS or PREGELIX! (saw $1)" 1>&2
    exit 1
fi
if [ $1 == "HYRACKS" ]; then
    my_home="$genomix_home/hyracks"
    cmd=( "${my_home}/bin/hyracksnc" )
else 
    my_home="$genomix_home/pregelix"
    cmd=( "${my_home}/bin/pregelixnc" )

    # cleanup temporary stores dirs
    . "$my_home/conf/stores.properties"
    for store_dir in $(echo $store | tr "," "\n"); do
        rm -rf $store_dir
        mkdir -p $store_dir
    done
fi

my_name=`hostname`
#Get the IP address of the cc
cchost_name=`cat conf/master`
cchost=`ssh -n ${cchost_name} "${genomix_home}/bin/getip.sh"`

# import cluster properties
. "$my_home/conf/cluster.properties"

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

ipaddr=`bin/getip.sh`

#Get node ID
nodeid=`hostname`

export JAVA_HOME=$JAVA_HOME
export JAVA_OPTS=$NCJAVA_OPTS

#Enter the temp dir
cd $NCTMP_DIR
cmd+=( -cc-host $cchost -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $ipaddr
       -data-ip-address $ipaddr -result-ip-address $ipaddr -node-id $nodeid 
       -iodevices "${IO_DIRS}" );

printf "\n\n\n********************************************\nStarting NC with command %s\n\n" "$cmd" >> "$NCLOGS_DIR"/$nodeid.log

#Launch nc


# start the nc process and make sure it exists after a few seconds
timeout=1
coproc start_fd { "${cmd[@]}" >> "$NCLOGS_DIR/$nodeid.log" 2>&1 ; }
server_pid=$!
set +e
read -t $timeout -u "${start_fd[0]}"
read_result=$?
set -e
if (($read_result > 128)); then
    # timeout => server is up and running
    echo $server_pid
else
    echo >&2 "Failure detected starting NC! (should have PID $server_pid)"
    tail >&2 -15 "$NCLOGS_DIR/$nodeid.log"
    exit 1
fi
exit 0
