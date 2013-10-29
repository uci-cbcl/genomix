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
#set -x

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

if [[ $# != 1 || ("$1" != "HYRACKS" && "$1" != "PREGELIX") ]]; then
    echo "please provide a cluster type as HYRACKS or PREGELIX! (saw $1)" 1>&2
    exit 1
fi
if [ $1 == "HYRACKS" ]; then
    MY_HOME="$GENOMIX_HOME/hyracks"
else 
    MY_HOME="$GENOMIX_HOME/pregelix"
fi

PID_FILE="$MY_HOME/ncs.pid"
if [ -e $PID_FILE ]; then
    echo "NC's are already running (seen in $PID_FILE)" 1&>2
    exit 1
fi

pid_re="^[0-9]+$"
for slave in `cat conf/slaves`
do
   # ssh to the slave machine, starting the NC and capturing its hostname and PID
   SLAVE_PID=$( ssh -n $slave "export JAVA_HOME=${JAVA_HOME} &>/dev/null && export NCJAVA_OPTS="$NCJAVA_OPTS" &>/dev/null && \"${GENOMIX_HOME}/bin/startnc.sh\" $1" )
   if [[ "$SLAVE_PID" =~ $pid_re ]] ; then
     printf "slave: %s\t%s\n"  "$slave" "$SLAVE_PID"
     printf "%s\t%s\n"  "$slave" "$SLAVE_PID" >> $PID_FILE
   else
     echo "failed to start $slave : expected PID, got $SLAVE_PID"
   fi
done
