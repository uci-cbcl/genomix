#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

for i in `cat conf/slaves`
do
   ssh $i "cd \"${GENOMIX_HOME}\"; export JAVA_HOME=${JAVA_HOME}; bin/startnc.sh" || { echo "Couldn't start node $i" ; exit 1; }
done
