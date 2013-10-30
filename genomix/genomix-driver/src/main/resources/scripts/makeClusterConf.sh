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
set -x

# args are cluster type and number of partitions per machine
if [ $# != 2 ]; then
    echo "Please specify exactly 2 arguments, (HYRACKS | PREGELIX) threads_per_machine " 1>&2
    exit 1;
fi
if [ "$1" == "HYRACKS" ]; then
    cluster_type="hyracks"
elif [ "$1" == "PREGELIX" ]; then
    cluster_type="pregelix"
else
    echo "unknown cluster type $1" 1>&2
    exit 1
fi
threads_per_machine="$2"

genomix_home="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$genomix_home"


# get generic cluster properties to templatize
. conf/cluster.properties

# make lengths of $IO_DIRS and $store equal to threads_per_machine but
# distribute to all the available $DISKS by round-robinning them
IO_DIRS=""
store=""
delim=""
IFS=',' read -ra disk_array <<< "$DISKS"  # separate $DISKS into an array
for i in `seq $threads_per_machine`; do
    disk_index=$(( ($i - 1) % ${#disk_array[@]} ))
    device=${disk_array[$disk_index]}
    IO_DIRS+="$delim""$device/$cluster_type/io_dir-"$i
    store+="$delim""$device/$cluster_type/store-"$i
    delim=","
done

# write the generated conf file to (hyracks|pregelix)/conf/cluster.properties
conf_home="$genomix_home/$cluster_type/conf"
mkdir -p $conf_home

cat > "$conf_home/cluster.properties" <<EOF
CCTMP_DIR="$WORKPATH/$cluster_type/cc"
CCLOGS_DIR="$WORKPATH/$cluster_type/cc/logs"
NCTMP_DIR="$WORKPATH/$cluster_type/nc"
NCLOGS_DIR="$WORKPATH/$cluster_type/nc/logs"
IO_DIRS="$IO_DIRS"
EOF

if [ "$cluster_type" == "hyracks" ]; then
    cat >> "$conf_home/cluster.properties" <<EOF
CC_CLIENTPORT=$HYRACKS_CC_CLIENTPORT
CC_CLUSTERPORT=$HYRACKS_CC_CLUSTERPORT
CC_HTTPPORT=$HYRACKS_CC_HTTPPORT
CC_DEBUG_PORT=$HYRACKS_CC_DEBUG_PORT
NC_DEBUG_PORT=$HYRACKS_NC_DEBUG_PORT
EOF
elif [ "$cluster_type" == "pregelix" ]; then
    cat >> "$conf_home/cluster.properties" <<EOF
CC_CLIENTPORT=$PREGELIX_CC_CLIENTPORT
CC_CLUSTERPORT=$PREGELIX_CC_CLUSTERPORT
CC_HTTPPORT=$PREGELIX_CC_HTTPPORT
CC_DEBUG_PORT=$PREGELIX_CC_DEBUG_PORT
NC_DEBUG_PORT=$PREGELIX_NC_DEBUG_PORT
EOF
    cat > "$conf_home/stores.properties" <<EOF
store="$store"
EOF
else
    echo "unrecognized cluster-type $cluster_type" 1>&2 
    exit 1
fi
