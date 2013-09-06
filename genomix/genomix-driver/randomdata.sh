#!/bin/bash

#set -o pipefail
#set -e
set -x

if [ $# -ne 5 ]; then
    echo "please provide 5 parameters: infile.readids numlines numfiles outdir and cmd" 
    echo "for example:   $0 /ffs/test/cbcl/wbiesing/testdata/5k_assemblathon_readids/5k_assemblathon.readids 100 5 /ffs/test/cbcl/wbiesing/testdata/5k_assemblathon_randomreadids  \"bin/genomix -kmerLength 55 -localOutput ~/result/500k_reads_P4 -pipelineOrder BUILD_HADOOP,MERGE -followsGraphBuild true -localInput \""
    exit 1
fi

INFILE=$1
NUMLINES=$2
NUM_FILES=$3
OUTDIR=$4
CMD=$5

rm -rf $OUTDIR

for i in `seq 1 $NUM_FILES`;
do
    mkdir -p $OUTDIR/random_set_$i || (echo "chouldn't make the output dir $OUTDIR/random_set_$i" && exit 1)
    sort -R $INFILE | head -n $NUMLINES > $OUTDIR/random_set_$i/random.readid
    eval "$CMD $OUTDIR/random_set_$i/random.readid"  || exit 1
done



