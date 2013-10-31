#!/bin/bash
#------------------------------------------------------------------------
# Copyright 2009-2013 by The Regents of the University of California
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License from
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------
set +e  # do NOT stop if a subshell fails
set +o pipefail
# set -x

genomix_home="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$genomix_home"

if [[ $# != 1 || ("$1" != "HYRACKS" && "$1" != "PREGELIX") ]]; then
    echo >&2 "please provide a cluster type as HYRACKS or PREGELIX! (saw \"$1\")"
    exit 1
fi
if [ $1 == "HYRACKS" ]; then
    my_home="$genomix_home/hyracks"
else 
    my_home="$genomix_home/pregelix"
fi

pid_file="$my_home/ncs.pid"

if [ ! -e $pid_file ]; then
    echo >&2 "No pid file for NC's! (expected $pid_file)"
    exit 1
else
    # PID file is one line per machine with format "host\tPID"
    while IFS=$'\t' read -r -a slave_pid; do
        ssh -n "${slave_pid[0]}" "kill -9 ${slave_pid[1]}"
        if [ $? == 0 ]; then
            echo "Stopped slave: ${slave_pid[0]}"$'\t'"${slave_pid[1]}"
        else
            echo "Failed to stop slave:" ${slave_pid[0]} ${slave_pid[1]}
        fi
    done < $pid_file
    rm $pid_file
fi
