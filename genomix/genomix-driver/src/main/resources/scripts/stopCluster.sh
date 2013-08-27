#!/bin/bash

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

bin/stopAllNCs.sh
sleep 2
bin/stopcc.sh
