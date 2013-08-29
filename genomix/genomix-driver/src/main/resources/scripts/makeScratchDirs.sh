#!/bin/bash
set -x

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

. conf/cluster.properties
. conf/stores.properties

for i in `cat conf/slaves`
do
   DIRS=`echo $store | tr "," " "`
   # ssh to the slave machine and capture its hostname and 
   ssh -n $i "mkdir -p $DIRS"

   DIRS=`echo $IO_DIRS | tr "," " "`
   ssh -n $i "mkdir -p $DIRS"
done
