#!/bin/bash

GENOMIX_HOME="$( dirname "$( dirname "$( readlink -e "${BASH_SOURCE[0]}" )" )" )"  # parent dir of absolute script path
cd "$GENOMIX_HOME"

bin/stopAllNCs.sh
sleep 2
bin/stopcc.sh
