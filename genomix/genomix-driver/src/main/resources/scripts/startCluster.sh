#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"
echo `pwd`

bin/startcc.sh || { echo "Failed to start CC!"; exit 1; }
sleep 5
bin/startAllNCs.sh || { echo "Failed to start all NC's!"; exit 1; }

