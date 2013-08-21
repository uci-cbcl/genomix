#!/bin/bash
set -e
set -o pipefail

hostname

#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CURRENT_PATH=`pwd`
CCHOST=`ssh ${CCHOST_NAME} "cd ${CURRENT_PATH}; bin/getip.sh"`

#Import cluster properties
. conf/cluster.properties
. conf/debugnc.properties

#Clean up temp dir

#rm -rf $NCTMP_DIR2
mkdir -p $NCTMP_DIR2

#Clean up log dir
#rm -rf $NCLOGS_DIR2
mkdir -p $NCLOGS_DIR2


#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS2 | tr "," "\n")
for io_dir in $io_dirs
do
	#rm -rf $io_dir
	mkdir $io_dir
done

#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME

#Get OS
IPADDR=`bin/getip.sh`

#Get node ID
NODEID=`hostname | cut -d '.' -f 1`

#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS2

GENOMIX_HOME=`pwd`

#Enter the temp dir
cd $NCTMP_DIR2

#Launch hyracks nc
${GENOMIX_HOME}/bin/genomixnc -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -result-ip-address $IPADDR -node-id $NODEID -iodevices "${IO_DIRS}" &> $NCLOGS_DIR/$NODEID.log &
