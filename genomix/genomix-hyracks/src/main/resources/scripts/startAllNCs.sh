#!/bin/bash
set -e
set -o pipefail

GENOMIX_PATH=`pwd`

for i in `cat conf/slaves`
do
   ssh $i "cd ${GENOMIX_PATH}; export JAVA_HOME=${JAVA_HOME}; bin/startnc.sh" || { echo "Couldn't start node $i" ; exit 1; }
done
