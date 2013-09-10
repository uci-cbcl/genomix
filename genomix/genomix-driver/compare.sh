#!/bin/bash

#set -x

if [ $# -ne 2 ]; then
	echo "Please provide 2 parameters: infile.readids, outdir"
	echo "For example:	$0 /ffs/test/cbcl/anbangx/data/tail600000 /ffs/test/cbcl/anbangx/data/graphbuild"
	exit 1
fi

INFILE=$1
OUTDIR=$2

rm -rf $OUTDIR
mkdir -p $OUTDIR

$CMD="/data/users/anbangx/hyracks/genomix/genomix-driver/target/appassembler/bin/genomix"
#run original dataset by hyracks
/data/users/anbangx/hyracks/genomix/genomix-driver/target/appassembler/bin/genomix -kmerLength 55 -localInput $INFILE -localOutput $OUTDIR/original-hyracks -pipelineOrder BUILD_HYRACKS 2>&1 | tee $OUTDIR/original-hyracks/log

#run original dataset by hadoop
/data/users/anbangx/hyracks/genomix/genomix-driver/target/appassembler/bin/genomix -kmerLength 55 -localInput $INFILE -localOutput $OUTDIR/original-hadoop -pipelineOrder BUILD_HADOOP 2>&1 | tee $OUTDIR/original-hadoop/log

#random the data 
#sort -R $INFILE | head 600000 > $INFILE+"_random"

#$#RANDOMINPUT 
#run original dataset by hyracks
/data/users/anbangx/hyracks/genomix/genomix-driver/target/appassembler/bin/genomix -kmerLength 55 -localInput ${INFILE}_random -localOutput $OUTDIR/random-hyracks -pipelineOrder BUILD_HYRACKS 2>&1 | tee $OUTDIR/random-hyracks/log

#run original dataset by hadoop
/data/users/anbangx/hyracks/genomix/genomix-driver/target/appassembler/bin/genomix -kmerLength 55 -localInput ${INFILE}_random -localOutput $OUTDIR/random-hadoop -pipelineOrder BUILD_HADOOP 2>&1 | tee $OUTDIR/random-hadoop/log

#sort the output and diff
sort -n $OUTDIR/original-hyracks/data >  $OUTDIR/original-hyracks/sort
sort -n $OUTDIR/original-hadoop/data >  $OUTDIR/original-hadoop/sort
sort -n $OUTDIR/random-hyracks/data >  $OUTDIR/random-hyracks/sort
sort -n $OUTDIR/random-hadoop/data >  $OUTDIR/random-hadoop/sort

diff $OUTDIR/original-hyracks/sort $OUTDIR/random-hyracks/sort > ~/diff/original-random_hyracks
diff $OUTDIR/original-hadoop/sort $OUTDIR/random-hadoop/sort > ~/diff/original-random_hadoop
diff $OUTDIR/original-hyracks/sort $OUTDIR/original-hadoop/sort > ~/diff/hyracks-hadoop_original
diff $OUTDIR/random-hyracks/sort $OUTDIR/random-hadoop/sort > ~/diff/hyracks-hadoop_random


