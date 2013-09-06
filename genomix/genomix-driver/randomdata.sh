#!/bin/bash

#set -o pipefail
#set -e
set -x

[ $# -lt 5 ] && (echo "please provide 5 parameters: infile.readids numlines numfiles outdir and cmd"; exit 1)

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



