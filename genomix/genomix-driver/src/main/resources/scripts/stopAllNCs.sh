#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

for i in `cat conf/slaves`
do
   ssh $i "cd ${GENOMIX_HOME}; bin/stopnc.sh" || { echo "Failed to stop slave $i"; }  # don't stop killing the other jobs when we fail
done
