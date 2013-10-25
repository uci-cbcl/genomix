#!/bin/bash

#set -o pipefail
#set -e
#set -x

if [ $# -ne 4 ]; then
    echo "please provide 4 parameters: hdfsInput, localGraphOutput, startSeeds and numOfHops" 
    echo "for example:   $0 /user/anbangx/test-extract-graph ~/graph/ CCCTCACTCTCCCCTAACGCCCCTGCCTCCCAGACCCACGACTCACAGGAGGAGC 2"
    exit 1
fi

OUTDIR=$2
rm -rf $OUTDIR
mkdir -p $OUTDIR

# cd genomix/genomix-pregelix/
target/appassembler/bin/genomix -kmerLength 55 -pipelineOrder EXTRACT_SUBGRAPH -hdfsInput $1 -localGraphOutput $OUTDIR -hdfsWorkPath /user/anbangx/genomix-out-extract-subgraph -startSeedToExtractSubgraph $3 -numHopsToExtractSubgraph $4
# cd ../..
