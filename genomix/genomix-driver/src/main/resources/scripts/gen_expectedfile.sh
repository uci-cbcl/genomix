#!/bin/bash
set -e

# generate expected file 
genomix_home="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"

cd "$genomix_home"
echo "The genemix_home is: `pwd -P`"

input="$genomix_home/../../../genomix-pregelix/src/test/resources/TestSet/Reads"
output="$genomix_home/../../../genomix-pregelix/src/test/resources/TestSet/expected"
onlydir="$genomix_home/../../../genomix-pregelix/src/test/resources/"

# automatically regenerate job conf
function regenerate_job_conf{
	cd "$genomix_home/../../../genomix-pregelix"
	java -classpath target/genomix-pregelix-0.2.10-SNAPSHOT-jar-with-dependencies.jar:target/test-classes/ edu.uci.ics.genomix.pregelix.jobgen.JobGenerator
}

# automatically regenerate only file
function regenerate_only_file{
	cd "$onlydir"
	rm -f only_*
	for pattern in MERGE LOW_COVERAGE SPLIT_REPEAT BUBBLE SCAFFOLD UNROLL_TANDEM CHECK_SYMMETRY PLOT_SUBGRAPH TIP_ADD BRIDGE_ADD BUBBLE_ADD TIP_REMOVE BRIDGE BFS; do
		echo "$pattern.xml" >> only_$pattern.txt
	done
}

# automatically generate expected file
function generate_expect_file{
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

	# TIP_REMOVE need to follow TIP_ADD to generate expected file
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

	# BRIDGE_REMOVE need to follow BRIDGE_ADD to generate expected file
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
}

echo "Expected file is necessary to regenerated, but user can have the choice to regenerate the job conf and only_file by specify parameters."
echo "ex. $0 regenerateJobConf regenerateOnlyFile"
echo "This can also regenerate jobConf and onlyFile"

if [ $# == 2 ]; then
	if [ "$1" == "regenerateJobConf" && "$2" == "regenerateOnlyFile" ]; then
		echo "Automatically regenerating job conf..."
		regenerate_job_conf
		echo "Automatically regenerating only file..."
		regenerate_only_file 
elif [ $# == 1 ]; then
	if [ "$1" == "regenerateJobConf" ]; then
		echo "Automatically regenerating job conf..."
		regenerate_job_conf
	elif [ "$1" == "regenerateOnlyFile"]; then
		echo "Automatically regenerating only file..."
		regenerate_only_file
fi

echo "Automatically generating expected file..."
generate_expect_file
