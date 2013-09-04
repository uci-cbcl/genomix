#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

. conf/cluster.properties

PID=$1
kill -9 $PID

#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir/*
done

#Clean up NC temp dir but keep the default logs directory
shopt -s extglob
rm -rf $NCTMP_DIR/!(logs)
