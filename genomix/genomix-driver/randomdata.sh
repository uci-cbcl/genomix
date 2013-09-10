#!/bin/bash

#set -o pipefail
#set -e
#set -x

if [ $# -ne 5 ]; then
    echo "please provide 5 parameters: infile.readids numlines numfiles outdir and cmd" 
    echo "for example:   $0 /ffs/test/cbcl/anbangx/data/ 100 5 /ffs/test/cbcl/anbangx/randomset  \"/data/users/anbangx/hyracks/genomix/genomix-driver/target/appassembler/bin/genomix -kmerLength 55 -pipelineOrder BUILD_HYRACKS,MERGE \""
    exit 1
fi

INFILE=$1
NUMLINES=$2
NUM_FILES=$3
OUTDIR=$4
CMD=$5

rm -rf $OUTDIR
mkdir -p $OUTDIR/logs/error

for i in `seq 1 $NUM_FILES`;
do
    mkdir -p $OUTDIR/random_set_$i || (echo "chouldn't make the output dir $OUTDIR/random_set_$i" && exit 1)
    #sort -R $INFILE | head -n $NUMLINES > $OUTDIR/random_set_$i/random.readid
    shuf -n $NUMLINES $INFILE > $OUTDIR/random_set_$i/random.readid
    echo "$CMD -localInput $OUTDIR/random_set_$i/random.readid -localOutput ~/result/randomset -hdfsWorkPath /user/anbangx/result/random_set_$i 2>&1 | tee $OUTDIR/logs/random_set_$i"
    eval "$CMD -localInput $OUTDIR/random_set_$i/random.readid -localOutput ~/result/randomset -hdfsWorkPath /user/anbangx/result/random_set_$i" 2>&1 | tee $OUTDIR/logs/random_set_$i
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        mv $OUTDIR/logs/random_set_$i $OUTDIR/logs/error/random_set_$i
    fi
done


