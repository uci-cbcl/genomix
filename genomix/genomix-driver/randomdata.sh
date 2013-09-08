#!/bin/bash

#set -o pipefail
#set -e
#set -x

if [ $# -ne 5 ]; then
    echo "please provide 5 parameters: infile.readids numlines numfiles outdir and cmd" 
    echo "for example:   $0 /data/users/anbangx/testdata/5k_assemblathon_readids/5k_assemblathon.readids 100 5 ~/subset  \"/data/users/anbangx/hyracks/genomix/genomix-driver/target/appassembler/bin/genomix -kmerLength 55 -localOutput ~/result/500k_reads_P4 -pipelineOrder BUILD_HADOOP,MERGE -localInput \""
    exit 1
fi

INFILE=$1
NUMLINES=$2
NUM_FILES=$3
OUTDIR=$4
CMD=$5

rm -rf $OUTDIR
rm -rf $OUTDIR/logs
mkdir -p $OUTDIR/logs

for i in `seq 1 $NUM_FILES`;
do
    mkdir -p $OUTDIR/random_set_$i || (echo "chouldn't make the output dir $OUTDIR/random_set_$i" && exit 1)
    sort -R $INFILE | head -n $NUMLINES > $OUTDIR/random_set_$i/random.readid
    eval "$CMD $OUTDIR/random_set_$i/random.readid" 2>&1 | tee $OUTDIR/logs/random_set_$i
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        rm -rf $OUTDIR/logs/random_set_$i
    fi
done



