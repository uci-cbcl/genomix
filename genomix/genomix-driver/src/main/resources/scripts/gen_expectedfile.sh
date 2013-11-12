#!/bin/bash
set -e

# generate expected file 
genomix_home="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"

cd "$genomix_home"
#cd "../../../genomix-pregelix/src/test/resources/TestSet/expected/Reads/"
#echo "`pwd -P`"

input="$genomix_home/../../../genomix-pregelix/src/test/resources/TestSet/Reads"
output="$genomix_home/../../../genomix-pregelix/src/test/resources/TestSet/expected"

rm -rf $output
mkdir -p $output

cd "$input"
for pattern in MERGE TIP_REMOVE LOW_COVERAGE BUBBLE BRIDGE SPLITREPEAT SCAFFOLD UNROLL_TANDEM CHECK_SYMMETRY PLOT_SUBGRAPH; do
        echo "Patern: $pattern"
	pushd $pattern
	for file in *; do
		echo "Running cmd = ($genomix_home/bin/genomix -kmerLength 3 -localInput $file -pipelineOrder BUILD,$pattern -localOutput $output/$pattern/$file -randomSeed 500 -runLocal)"
		$genomix_home/bin/genomix -kmerLength 3 -localInput $file -pipelineOrder BUILD,$pattern -localOutput $output/$pattern/$file -randomSeed 500 -plotSubgraph_startSeed "AAT" -plotSubgraph_numHops 1
		#sleep 30
	done
	popd
done

