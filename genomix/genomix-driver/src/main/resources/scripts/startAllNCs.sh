#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( dirname "$( readlink -e "${BASH_SOURCE[0]}" )" )" )"  # parent dir of absolute script path
cd "$GENOMIX_HOME"

for i in `cat conf/slaves`
do
   ssh $i "cd \"${GENOMIX_HOME}\"; export JAVA_HOME=${JAVA_HOME}; bin/startnc.sh" || { echo "Couldn't start node $i" ; exit 1; }
done
