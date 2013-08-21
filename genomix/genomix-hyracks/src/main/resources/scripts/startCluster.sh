#!/bin/bash
set -e
set -o pipefail

bin/startcc.sh || { echo "Failed to start CC!"; exit 1; }
sleep 5
bin/startAllNCs.sh || { echo "Failed to start all NC's!"; exit 1; }

