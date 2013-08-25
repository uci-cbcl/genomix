#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( dirname "$( readlink -e "${BASH_SOURCE[0]}" )" )" )"  # parent dir of absolute script path
cd "$GENOMIX_HOME"

bin/startcc.sh || { echo "Failed to start CC!"; exit 1; }
sleep 5
bin/startAllNCs.sh || { echo "Failed to start all NC's!"; exit 1; }

