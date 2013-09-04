#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"



MY_NAME=`hostname`
#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CCHOST=`ssh ${CCHOST_NAME} "cd \"${GENOMIX_HOME}\"; bin/getip.sh"`

#Import cluster properties
. conf/cluster.properties

#Clean up temp dir

#rm -rf $NCTMP_DIR
mkdir -p $NCTMP_DIR

#Clean up log dir
#rm -rf $NCLOGS_DIR
mkdir -p $NCLOGS_DIR


#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	#rm -rf $io_dir
	mkdir -p $io_dir
done

#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME

IPADDR=`bin/getip.sh`
#echo $IPADDR

#Get node ID
NODEID=`hostname | cut -d '.' -f 1`

#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS

#Enter the temp dir
cd $NCTMP_DIR

if [ "$1" == "HYRACKS" ]; then
  NCTYPE="hyracksnc"
elif [ "$1" == "PREGELIX" ]; then
  NCTYPE="pregelixnc"
else
  echo "unknown NC type $1" 1>&2
  exit 1
fi

CMD="\"${GENOMIX_HOME}/bin/$NCTYPE\" -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -result-ip-address $IPADDR -node-id $NODEID -iodevices \"${IO_DIRS}\""

printf "\n\n\n********************************************\nStarting NC with command %s\n\n" "$CMD" >> "$NCLOGS_DIR"/$NODEID.log

#Launch nc
eval "$CMD >> \"$NCLOGS_DIR/$NODEID.log\" 2>&1  &"

echo $!  # write PID of bg'ed script

