#!/bin/bash
set -e
set -o pipefail

GENOMIX_PATH=`pwd`

for i in `cat conf/slaves`
do
   ssh $i "cd ${GENOMIX_PATH}; bin/stopnc.sh" || { echo "Failed to stop slave $i"; }  # don't stop killing the other jobs when we fail
done
