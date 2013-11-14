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

# automatically generate some patterns that can be directly generated from graph building
cd "$input"
for pattern in MERGE LOW_COVERAGE SPLIT_REPEAT SCAFFOLD UNROLL_TANDEM CHECK_SYMMETRY PLOT_SUBGRAPH TIP_ADD BRIDGE_ADD; do
        echo "Patern: $pattern"
	pushd $pattern
	conf="$genomix_home/../../../genomix-pregelix/src/test/resources/jobs/$pattern.xml"
	for file in *; do
		cmd="$genomix_home/bin/genomix -kmerLength 3 -localInput $file -localOutput $output/$pattern/$file -confInput $conf -pipelineOrder BUILD,$pattern 2>&1 | tee $output/$pattern/$file.log"
		echo "Running cmd = ($cmd)"
		eval "$cmd"
		#sleep 30
	done
	# delete hdfs(here is current local directory) output folder
	rm -rf genomix_out
	popd
done

# TIP_REMOVE
pattern="TIP_REMOVE"
echo "Patern: $pattern"
pushd $pattern
conf="$genomix_home/../../../genomix-pregelix/src/test/resources/jobs/TIP_ADD.xml,$genomix_home/../../../genomix-pregelix/src/test/resources/jobs/$pattern.xml"
for file in *; do
	cmd="$genomix_home/bin/genomix -kmerLength 3 -localInput $file -localOutput $output/$pattern/$file -confInput $conf -pipelineOrder BUILD,TIP_ADD,$pattern"
	echo "Running cmd = ($cmd)"
        eval "$cmd" 
done
rm -rf genomix_out
popd

# BRIDGE_REMOVE
pattern="BRIDGE"
echo "Patern: $pattern"
pushd $pattern
conf="$genomix_home/../../../genomix-pregelix/src/test/resources/jobs/BRIDGE_ADD.xml,$genomix_home/../../../genomix-pregelix/src/test/resources/jobs/$pattern.xml"
for file in *; do
	cmd="$genomix_home/bin/genomix -kmerLength 3 -localInput $file -localOutput $output/$pattern/$file -confInput $conf -pipelineOrder BUILD,BRIDGE_ADD,$pattern"	
	echo "Running cmd = ($cmd)"
        eval "$cmd"
done
rm -rf genomix_out
popd

# clean out output
find $output/*/*/bin/ -name "*.crc" | xargs rm -f
find $output/*/*/ -name "*.crc" | xargs rm -f
find $output/*/*/bin/ -name "_temporary" | xargs rm -rf
