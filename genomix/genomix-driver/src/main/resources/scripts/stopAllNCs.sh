#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( dirname "$( readlink -e "${BASH_SOURCE[0]}" )" )" )"  # parent dir of absolute script path
cd "$GENOMIX_HOME"

for i in `cat conf/slaves`
do
   ssh $i "cd ${GENOMIX_HOME}; bin/stopnc.sh" || { echo "Failed to stop slave $i"; }  # don't stop killing the other jobs when we fail
done
